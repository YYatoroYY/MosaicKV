"""Sample committed space state at explicit completed-operation boundaries."""
import csv
import json
from pathlib import Path
import time

from space_overhead import snapshot
from stats_snapshot import require_node_stats


class SpaceTrace:
    def __init__(self, directory, scheme, every, total):
        if every <= 0 or total <= 0:
            raise ValueError('Space sampling interval and operation count must be positive')
        self.directory, self.scheme = Path(directory), scheme
        self.every, self.total = every, total
        self.start = time.monotonic()
        self.rows = []

    def due(self, completed):
        return completed > 0 and completed % self.every == 0 and completed < self.total

    def sample(self, completed, stage, read):
        if not 0 <= completed <= self.total:
            raise ValueError('Invalid space sample progress')
        if self.rows and completed < self.rows[-1]['completed_operations']:
            raise ValueError('Space progress went backwards')
        begin = time.monotonic()
        state = read()
        end = time.monotonic()
        require_node_stats(state, 'space-' + stage)
        row = dict(scheme=self.scheme, k=state['k'], m=state['m'], stage=stage,
                   completed_operations=completed, total_operations=self.total,
                   progress=completed / self.total, elapsed_seconds=end - self.start,
                   sample_seconds=end - begin, **snapshot(state, self.scheme))
        # Persist each sample, including the original state, so an interrupted
        # instrumented replay retains useful diagnostics but never passes validation.
        with (self.directory / 'space-trace-stats.jsonl').open('a', encoding='utf-8') as stream:
            stream.write(json.dumps(dict(completed_operations=completed, stage=stage, stats=state)) + '\n')
        with (self.directory / 'space-trace.csv').open('a', newline='', encoding='utf-8') as stream:
            writer = csv.DictWriter(stream, fieldnames=list(row))
            if not self.rows:
                writer.writeheader()
            writer.writerow(row)
        self.rows.append(row)
        return row
