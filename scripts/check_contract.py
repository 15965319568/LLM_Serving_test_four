"""Public format and consistency check. Does not compute expected solutions."""
import argparse
import csv
import json
from pathlib import Path


def check(input_dir, output_dir, schema_path=None):
    schema_path = schema_path or Path(__file__).resolve().parents[1] / 'docs/artifact-schema.json'
    schema = json.loads(schema_path.read_text(encoding='utf-8'))
    errors, tables = [], {}
    for name, spec in schema['csv'].items():
        path = output_dir / name
        if not path.is_file():
            errors.append(f'{name}: missing artifact')
            continue
        with path.open(encoding='utf-8', newline='') as stream:
            reader = csv.DictReader(stream)
            missing = set(spec['required']) - set(reader.fieldnames or [])
            if missing:
                errors.append(f'{name}: missing fields {sorted(missing)}')
                continue
            rows = list(reader)
        tables[name] = rows
        keys = [tuple(row[k] for k in spec['primary_key']) for row in rows]
        if keys != sorted(keys) or len(set(keys)) != len(keys):
            errors.append(f'{name}: primary keys must be unique and sorted')
        for field, choices in spec['enums'].items():
            invalid = sorted({str(row[field]) for row in rows} - set(choices))
            if invalid:
                errors.append(f'{name}: {field} must be in {choices}; found {invalid[:5]}')
    for kind in ('json', 'ndjson'):
        for name, required in schema[kind].items():
            try:
                payload = (output_dir/name).read_text(encoding='utf-8')
                objects = [json.loads(payload)] if kind == 'json' else [json.loads(line) for line in payload.splitlines() if line.strip()]
                if not objects or any(not isinstance(o, dict) or set(required)-set(o) for o in objects):
                    errors.append(f'{name}: required fields are {required}')
            except (OSError, ValueError):
                errors.append(f'{name}: missing or invalid {kind}')
    if errors:
        return errors
    decisions = {(r['window_id'], r['scenario_id']): r for r in tables['recovery_decisions.csv']}
    assignments = tables['recovery_assignments.csv']
    for key, decision in decisions.items():
        rows = [r for r in assignments if (r['window_id'], r['scenario_id']) == key]
        if decision['status'] == 'BLOCKED':
            if rows or decision['total_cost_cents'] != '' or decision['makespan_seconds'] != '' or decision['assigned_shards'] != '0':
                errors.append(f'{key}: BLOCKED must have no assignments, no cost/makespan and assigned_shards=0')
            continue
        try:
            if len(rows) != int(decision['required_shards']) or int(decision['assigned_shards']) != len(rows):
                errors.append(f'{key}: incomplete assignment count')
            if sum(int(r['cost_cents']) for r in rows) != int(decision['total_cost_cents']):
                errors.append(f'{key}: assignment costs do not reconcile')
            if not rows or max(int(r['cutover_seconds']) for r in rows) != int(decision['makespan_seconds']):
                errors.append(f'{key}: makespan does not reconcile')
            wave_ids = sorted({int(r['wave']) for r in rows})
            if wave_ids != list(range(1, len(wave_ids)+1)):
                errors.append(f'{key}: wave IDs must be consecutive from 1')
            previous = 0
            for wave in wave_ids:
                group = [r for r in rows if int(r['wave']) == wave]
                starts = {int(r['warmup_start_seconds']) for r in group}
                ends = {int(r['cutover_seconds']) for r in group}
                if len(starts) != 1 or len(ends) != 1 or min(starts) < previous or min(ends) <= min(starts):
                    errors.append(f'{key}: wave {wave} must share start/end and not overlap')
                previous = max(ends)
        except (ValueError, TypeError):
            errors.append(f'{key}: costs, wave IDs and schedule offsets must be integers')
    policy = json.loads((input_dir/'recovery_policy.json').read_text(encoding='utf-8'))
    for row in tables['execution_plan.csv']:
        decision = decisions.get((row['window_id'], policy['nominal_scenario']))
        if decision is None or row['recovery_status'] != decision['status']:
            errors.append(f"{row['window_id']}: execution recovery_status must use the nominal scenario")
        elif row['action'] != ('rollback' if row['status']=='ROLLBACK' and decision['status']=='EXECUTABLE' else 'escalate' if row['status']=='ROLLBACK' else 'observe'):
            errors.append(f"{row['window_id']}: execution action contradicts recovery result")
    return errors


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--input', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    try:
        errors = check(args.input, args.output)
    except (OSError, ValueError, KeyError, TypeError) as error:
        errors = [str(error)]
    for error in errors:
        print('FAIL: '+error)
    if not errors:
        print('Public format and consistency checks passed. Historical semantics and global optimality still require verification.')
    raise SystemExit(bool(errors))


if __name__ == '__main__':
    main()
