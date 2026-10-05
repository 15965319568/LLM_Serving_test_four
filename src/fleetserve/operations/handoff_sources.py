"""File transport adapters for the operational handover."""
import csv
import gzip
import json

def read_csv(path):
    with path.open(encoding='utf-8-sig', newline='') as stream:
        return list(csv.DictReader(stream))


def canonical(row):
    return json.dumps(row, sort_keys=True, separators=(',', ':'))


def latest(rows, sequence, fields):
    if not rows:
        return None
    newest = max(int(row[sequence]) for row in rows)
    finalists = [row for row in rows if int(row[sequence]) == newest]
    facts = {tuple(row[key] for key in fields) for row in finalists}
    if len(facts) != 1:
        raise ValueError('conflicting_authority')
    return finalists[0]


def observations(root, filenames):
    records, errors = [], []
    for filename in filenames:
        path = root / filename
        if path.suffix == '.csv':
            for row in read_csv(path):
                row['trace_time'] = float(row['trace_time'])
                row['payload'] = json.loads(row['payload'])
                records.append(row)
        else:
            opener = gzip.open if path.suffix == '.gz' else open
            with opener(path, 'rt', encoding='utf-8-sig') as stream:
                for line_number, line in enumerate(stream, 1):
                    if not line.strip():
                        continue
                    try:
                        records.append(json.loads(line))
                    except json.JSONDecodeError:
                        errors.append(dict(file=filename, line=line_number, code='invalid_json'))
    groups = {}
    for row in records:
        groups[row['observation_id']] = {canonical(row): row}
    return groups, sorted(errors, key=lambda r: (r['file'], r['line']))
