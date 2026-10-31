#!/usr/bin/env python3
"""Execute official YCSB, then independently check the exported resize history."""
import json
import csv
from pathlib import Path
import tempfile

from generate_ycsb import ROOT, convert, digest, generate, parser, rows, twitter_weights


def main():
    p = parser()
    args = p.parse_args()
    reports = []
    # --output is a parent for temporary cases, not a full-scale experiment.
    args.output.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="ycsb-test-", dir=args.output) as temporary:
        edges = Path(temporary) / "edge-histogram.csv"
        with edges.open("w", newline="") as stream:
            writer = csv.writer(stream)
            writer.writerow(["new_size_bytes", "requests"])
            writer.writerows((size, 1 if size in (1, 4096) else 0) for size in range(1, 4097))
        for name, distribution, width, initial, lo, hi in (
                ("zipf", "zipfian", 32, 64, 1, 4096),
                ("uniform", "uniform", 128, 256, 1, 4096),
                ("filter-topup", "uniform", 32, 1, 1, 2),
                ("twitter", "zipfian", 32, 64, 1, 4096),
                ("twitter-edges", "uniform", 32, 64, 1, 4096)):
            args.workload = "twitter" if name.startswith("twitter") else "random"
            args.twitter_histogram = edges if name == "twitter-edges" else None
            args.records, args.operations = 100, 1000
            args.output = Path(temporary) / name
            args.distribution, args.key_bytes = distribution, width
            args.initial_bytes, args.min_bytes, args.max_bytes = initial, lo, hi
            report = generate(args)
            workload = json.loads((args.output / "workload.json").read_text())
            current = workload["initial_lengths"].copy()
            assert len(current) == 100 and set(current.values()) == {initial}
            assert all(len(key.encode()) == width for key in current)
            raw_updates = []
            for key, size in rows(args.output / "run.tsv"):
                old = current[key]
                if old != size:
                    raw_updates.append(dict(key=key, old_bytes=old, new_bytes=size))
                    current[key] = size
            assert raw_updates == workload["updates"]
            current = workload["initial_lengths"].copy()
            # These checks match the existing update_traffic_compare.py input contract.
            for step in workload["updates"]:
                key, old, new = step["key"], step["old_bytes"], step["new_bytes"]
                assert current[key] == old and old != new and lo <= new <= hi
                current[key] = new
            assert len(workload["updates"]) == 1000
            assert report["growth_updates"] + report["shrink_updates"] == 1000
            assert report["same_size_updates"] == 0
            for filename, expected in report["files"].items():
                assert digest(args.output / filename) == expected
            assert "YCSB Client 0.17.0" in (args.output / "run-000.log").read_text()
            if name == "filter-topup":
                assert report["filtered_same_size_updates"] > 0
                assert (args.output / "run-001.log").exists()
            if name.startswith("twitter"):
                _, weights = twitter_weights(args)
                candidates = report["candidate_size_histogram"]
                assert all(weights[size - 1] > 0 for size in candidates)
                assert report["metadata"]["workload"] == "twitter"
                if name == "twitter-edges":
                    assert set(candidates) == {1, 4096}
                    assert .42 < candidates[1] / sum(candidates.values()) < .58
                else:
                    assert report["metadata"]["twitter_reference"]["source_sha256"] == json.loads(
                        (ROOT / "data/twitter_sizes.source.json").read_text())["sha256"]
                    observed = sum(n for size, n in candidates.items() if size <= 32) / sum(candidates.values())
                    expected = sum(weights[:32]) / sum(weights)
                    assert abs(observed - expected) < .08
            # An incomplete raw stream must fail instead of yielding a valid artifact.
            args.operations += 1
            try:
                convert(args, workload["metadata"])
            except ValueError:
                pass
            else:
                raise AssertionError("incomplete trace accepted")
            reports.append(dict(case=name, passed=True, operations=1000,
                                filtered=report["filtered_same_size_updates"]))
        # Conditioning on a range with only one supported size cannot sustain resizes.
        args.min_bytes, args.max_bytes = 1, 2
        try:
            twitter_weights(args)
        except ValueError:
            pass
        else:
            raise AssertionError("single-size histogram accepted")
        edges.write_text("new_size_bytes,requests\n1,-1\n2,3\n")
        try:
            twitter_weights(args)
        except ValueError:
            pass
        else:
            raise AssertionError("invalid histogram accepted")
        args.workload, args.update_kind = 'random', 'fixed'
        args.twitter_histogram = None
        args.output = Path(temporary) / 'fixed256'
        args.records, args.operations = 100, 1000
        args.initial_bytes = 256
        report = generate(args)
        fixed = json.loads((args.output / 'workload.json').read_text())
        assert len(fixed['updates']) == 1000
        assert all(step['old_bytes'] == step['new_bytes'] == 256 for step in fixed['updates'])
        assert report['same_size_updates'] == 1000 and report['filtered_same_size_updates'] == 0
        reports.append(dict(case='fixed256', passed=True, operations=1000, filtered=0))
    print(json.dumps(reports, indent=2))


if __name__ == "__main__":
    main()
