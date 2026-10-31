#!/usr/bin/env python3
"""Run official YCSB 0.17.0 and export EC KV initial_lengths + updates."""
import argparse
from collections import Counter
import csv
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import urllib.request
import zipfile

ROOT = Path(__file__).resolve().parent
MAVEN = "https://repo.maven.apache.org/maven2/"
ARTIFACTS = (
    "site/ycsb/core/0.17.0/core-0.17.0.jar",
    "org/apache/htrace/htrace-core4/4.1.0-incubating/htrace-core4-4.1.0-incubating.jar",
    "org/codehaus/jackson/jackson-core-asl/1.9.4/jackson-core-asl-1.9.4.jar",
    "org/codehaus/jackson/jackson-mapper-asl/1.9.4/jackson-mapper-asl-1.9.4.jar",
    "org/hdrhistogram/HdrHistogram/2.1.4/HdrHistogram-2.1.4.jar",
)


def digest(path, algorithm="sha256"):
    h = hashlib.new(algorithm)
    with Path(path).open("rb") as f:
        for block in iter(lambda: f.read(1024 * 1024), b""):
            h.update(block)
    return h.hexdigest()


def jars(args):
    if args.ycsb_home:
        lib = args.ycsb_home.resolve() / "lib"
        paths = [lib / Path(a).name for a in ARTIFACTS]
        if not all(p.is_file() for p in paths):
            raise ValueError("--ycsb-home must contain the official 0.17.0 lib/*.jar dependencies")
        return paths
    lib = args.cache.resolve() / "lib"
    lib.mkdir(parents=True, exist_ok=True)
    paths = []
    for artifact in ARTIFACTS:
        path = lib / Path(artifact).name
        if not path.exists():
            print("Downloading " + artifact, flush=True)
            with urllib.request.urlopen(MAVEN + artifact + ".sha1", timeout=60) as response:
                checksum = response.read().decode().split()[0]
            temporary = path.with_suffix(".part")
            with urllib.request.urlopen(MAVEN + artifact, timeout=120) as response, temporary.open("wb") as output:
                shutil.copyfileobj(response, output)
            if digest(temporary, "sha1") != checksum:
                raise ValueError("Maven checksum mismatch: " + artifact)
            temporary.replace(path)
        with zipfile.ZipFile(path) as archive:
            if archive.testzip() is not None:
                raise ValueError("corrupt JAR: " + str(path))
        paths.append(path)
    return paths


def rows(path):
    with Path(path).open() as stream:
        for line in stream:
            key, size = line.rstrip("\n").split("\t")
            yield key, int(size)


def twitter_weights(args):
    source = (args.twitter_histogram or ROOT / "data" / "twitter_sizes.csv").resolve()
    if args.workload != "twitter":
        if args.twitter_histogram:
            raise ValueError("--twitter-histogram requires --workload twitter")
        return None, None
    with source.open(newline="") as stream:
        reader = csv.DictReader(stream)
        if reader.fieldnames != ["new_size_bytes", "requests"]:
            raise ValueError("Twitter CSV requires new_size_bytes,requests")
        entries = [(int(row["new_size_bytes"]), int(row["requests"])) for row in reader]
    if [size for size, _ in entries] != list(range(1, 4097)) or any(n < 0 for _, n in entries):
        raise ValueError("Twitter CSV requires sizes 1..4096 exactly once in order, with nonnegative weights")
    weights = [n if args.min_bytes <= size <= args.max_bytes else 0 for size, n in entries]
    if sum(n > 0 for n in weights) < 2 or sum(weights) > 2**63 - 1:
        raise ValueError("Twitter histogram must have at least two positive sizes in the selected range and total <= 2^63-1")
    return source, weights


def convert(args, metadata):
    current = {}
    for key, size in rows(args.output / "load.tsv"):
        if not re.fullmatch(r"user[0-9]+", key) or len(key) != args.key_bytes or key in current:
            raise ValueError("invalid or duplicate load key: " + key)
        if size != args.initial_bytes:
            raise ValueError("unexpected initial length")
        current[key] = size
    if len(current) != args.records:
        raise ValueError("YCSB load was incomplete; inspect load.log")
    total = same = filtered = growth = shrink = 0
    sizes = Counter()
    temporary = args.output / "workload.json.partial"
    with temporary.open("w") as out:
        out.write('{"metadata":' + json.dumps(metadata) + ',"initial_lengths":{')
        for index, (key, size) in enumerate(current.items()):
            out.write(("," if index else "") + json.dumps(key) + ":" + str(size))
        out.write('},"updates":[')
        for key, size in rows(args.output / "run.tsv"):
            if key not in current or not args.min_bytes <= size <= args.max_bytes:
                raise ValueError("unknown key or invalid update length")
            old = current[key]
            if old == size:
                same += 1
                if args.update_kind != "fixed":
                    filtered += 1
                    continue
            out.write(("," if total else "") + json.dumps(
                dict(key=key, old_bytes=old, new_bytes=size), separators=(",", ":")))
            current[key] = size
            growth += size > old
            shrink += size < old
            sizes[size] += 1
            total += 1
        out.write("]}\n")
    if total != args.operations:
        raise ValueError("YCSB run was incomplete; inspect run.log")
    temporary.replace(args.output / "workload.json")
    return dict(passed=True, keys=len(current), operations=total,
                same_size_updates=same if args.update_kind == "fixed" else 0, filtered_same_size_updates=filtered,
                growth_updates=growth, shrink_updates=shrink,
                update_size_histogram=dict(sorted(sizes.items())))


def generate(args):
    if not 1 <= args.records <= 2**31 - 1 or not 1 <= args.operations <= 2**31 - 1:
        raise ValueError("records and operations must be 1..2147483647")
    if not 23 <= args.key_bytes <= 128:
        raise ValueError("hashed YCSB keys require key-bytes in 23..128")
    if args.update_kind == "fixed":
        if args.workload != "random":
            raise ValueError("fixed-size YCSB updates use --workload random")
        args.min_bytes = args.max_bytes = args.initial_bytes
    if not (1 <= args.initial_bytes <= 4096 and 1 <= args.min_bytes <= args.max_bytes <= 4096 and (args.update_kind == "fixed" or args.min_bytes < args.max_bytes)):
        raise ValueError("existing runner requires initial bytes 1..4096 and 1 <= min-bytes < max-bytes <= 4096")
    histogram_source, weights = twitter_weights(args)
    for tool in (args.java, args.javac):
        if not shutil.which(tool):
            raise ValueError("JDK required; command not found: " + tool)
    args.output = args.output.resolve()
    if args.output.exists():
        raise ValueError("output already exists; choose a new directory")
    dependencies = jars(args)
    args.output.mkdir(parents=True)
    histogram = None
    if weights is not None:
        histogram = args.output / "twitter_sizes.csv"
        with histogram.open("w", newline="") as stream:
            writer = csv.writer(stream, lineterminator="\n")
            writer.writerow(["new_size_bytes", "requests"])
            writer.writerows(enumerate(weights, 1))
    classes = args.output / "classes"
    classes.mkdir()
    classpath = os.pathsep.join(str(p) for p in dependencies)
    subprocess.run([args.javac, "-encoding", "UTF-8", "-cp", classpath, "-d", str(classes),
                    str(ROOT / "TraceDB.java"), str(ROOT / "TwitterWorkload.java")], check=True)
    common = dict(workload="ec.trace.TwitterWorkload" if weights is not None else "site.ycsb.workloads.CoreWorkload", recordcount=args.records,
        operationcount=args.operations, fieldcount=1, readallfields="true", writeallfields="true",
        readproportion=0, updateproportion=1, insertproportion=0, scanproportion=0,
        readmodifywriteproportion=0, requestdistribution=args.distribution,
        insertorder="hashed", zeropadding=args.key_bytes - 4, threadcount=1)
    invocations = []
    def run_phase(phase, name, operations):
        props = dict(common, fieldlengthdistribution="constant" if phase == "load" or args.update_kind == "fixed" else "uniform",
                     fieldlength=args.initial_bytes if phase == "load" else args.max_bytes,
                     minfieldlength=args.min_bytes, operationcount=operations)
        propfile = args.output / (name + ".properties")
        propfile.write_text("".join(f"{key}={value}\n" for key, value in props.items()))
        cmd = [args.java, "-Xmx1g", "-cp", str(classes) + os.pathsep + classpath,
               "site.ycsb.Client", "-load" if phase == "load" else "-t", "-db", "ec.trace.TraceDB",
               "-P", str(propfile), "-p", "trace.phase=" + phase,
               "-p", "trace.output=" + str(args.output / (name + ".tsv"))]
        if histogram is not None:
            cmd += ["-p", "trace.histogram=" + str(histogram)]
        invocations.append(cmd)
        print("Running official YCSB " + name, flush=True)
        with (args.output / (name + ".log")).open("w") as log:
            subprocess.run(cmd, stdout=log, stderr=subprocess.STDOUT, check=True)
    run_phase("load", "load", args.operations)
    current = dict(rows(args.output / "load.tsv"))
    if len(current) != args.records:
        raise ValueError("YCSB load incomplete; inspect load.log")
    accepted = 0
    with (args.output / "run.tsv").open("w") as combined:
        for batch in range(100):
            remaining = args.operations - accepted
            name = f"run-{batch:03d}"
            run_phase("run", name, remaining)
            count = 0
            for key, size in rows(args.output / (name + ".tsv")):
                if key not in current or not args.min_bytes <= size <= args.max_bytes:
                    raise ValueError("invalid YCSB update")
                combined.write(f"{key}\t{size}\n")
                if args.update_kind == "fixed" or current[key] != size:
                    current[key] = size
                    accepted += 1
                count += 1
            if count != remaining:
                raise ValueError("YCSB run incomplete; inspect " + name + ".log")
            if accepted == args.operations:
                break
        else:
            raise ValueError("could not generate enough resizes after 100 batches")
    del current
    metadata = dict(update_kind=args.update_kind, kind="official_ycsb_updates", workload=args.workload, ycsb_version="0.17.0", keys=args.records,
        operations=args.operations, key_bytes=args.key_bytes, initial_value_bytes=args.initial_bytes,
        min_value_bytes=args.min_bytes, max_value_bytes=args.max_bytes,
        key_distribution=args.distribution, update_proportion=1,
        length_distribution=("project Twitter weighted CSV via CoreWorkload extension" if weights is not None else "YCSB uniform inclusive") + ", conditioned on new length != current length",
        filtering="equal-size proposals retained in run.tsv but omitted from workload.json; additional YCSB batches fill the target count",
        reproducibility="YCSB uses ThreadLocalRandom; reuse exported workload.json and its SHA-256",
        payload="only key/order/length exported; existing experiment runners synthesize value contents")
    if args.update_kind == "fixed":
        metadata.update(length_distribution="constant value length", filtering="none; all equal-size updates retained")
    if histogram is not None:
        metadata["twitter_reference"] = dict(source=histogram_source.name,
            source_sha256=digest(histogram_source), selected_histogram_sha256=digest(histogram),
            total_weight=sum(weights), selected_range=[args.min_bytes, args.max_bytes],
            interpretation="synthetic length-distribution workload; not original Twitter key/time/order replay")
    summary = convert(args, metadata)
    if weights is not None:
        raw_sizes = Counter(size for _, size in rows(args.output / "run.tsv"))
        summary["candidate_size_histogram"] = dict(sorted(raw_sizes.items()))
        if any(weights[size - 1] == 0 for size in raw_sizes):
            raise ValueError("YCSB emitted a size outside histogram support")
    summary.update(metadata=metadata, commands=invocations,
        java_version=subprocess.run([args.java, "-version"], capture_output=True, text=True, check=True).stderr,
        dependency_sha256={p.name: digest(p) for p in dependencies},
        source_sha256={p: digest(ROOT / p) for p in ("generate_ycsb.py", "TraceDB.java", "TwitterWorkload.java")},
        files={p.name: digest(p) for p in sorted(args.output.iterdir())
               if p.suffix in (".tsv", ".properties", ".json", ".log", ".csv")})
    (args.output / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
    print(json.dumps({k: summary[k] for k in ("passed", "keys", "operations", "same_size_updates")}))
    print(args.output / "workload.json")
    return summary


def parser():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--update-kind", choices=["resize", "fixed"], default="resize")
    p.add_argument("--workload", choices=["random", "twitter"], default="random",
                   help="uniform lengths or the bundled project Twitter length reference")
    p.add_argument("--twitter-histogram", type=Path, help="override bundled CSV; sizes 1..4096, columns new_size_bytes,requests")
    p.add_argument("--records", type=int, default=10000)
    p.add_argument("--operations", type=int, default=100000)
    p.add_argument("--key-bytes", type=int, default=32)
    p.add_argument("--initial-bytes", type=int, default=64)
    p.add_argument("--min-bytes", type=int, default=1)
    p.add_argument("--max-bytes", type=int, default=4096)
    p.add_argument("--distribution", choices=["zipfian", "uniform"], default="zipfian")
    p.add_argument("--ycsb-home", type=Path, help="existing official YCSB 0.17.0 directory; otherwise download core JARs")
    p.add_argument("--cache", type=Path, default=ROOT / ".cache" / "ycsb-0.17.0")
    p.add_argument("--java", default="java")
    p.add_argument("--javac", default="javac")
    p.add_argument("--output", type=Path, required=True)
    return p


if __name__ == "__main__":
    cli = parser()
    try:
        generate(cli.parse_args())
    except (ValueError, OSError, subprocess.CalledProcessError) as exc:
        cli.exit(1, str(exc) + "\n")
