"""Recovery planner retained from the single-region dashboard prototype."""
from pathlib import Path
from .io import read_csv, read_json, read_ndjson


def plan_recovery(root, data, output, write_csv):
    root, output = Path(root), Path(output)
    policy = read_json(root / 'recovery_policy.json')
    workloads = read_csv(root / 'recovery_workloads.csv')
    profiles = read_csv(root / 'placement_profiles.csv')
    events = read_ndjson(root / 'control_events.ndjson')
    evidence, options, assignments, decisions = [], [], [], []
    latest = {}
    for profile in sorted(profiles, key=lambda p: int(p['revision'])):
        latest[profile['profile_id']] = profile
    for window in data['policies']['windows']:
        wid = window['window_id']
        for event in events:
            evidence.append(dict(window_id=wid, event_id=event['event_id'], permit_id=event['permit_id'], disposition='active'))
        for scenario in policy['scenarios']:
            cost = 0
            for shard in workloads:
                for pid, profile in latest.items():
                    options.append(dict(window_id=wid, scenario_id=scenario['scenario_id'], shard_id=shard['shard_id'],
                                        profile_id=pid, selected_revision=profile['revision'], eligible='true',
                                        reason_codes='', capacity_milli_tokens_s=999999))
                profile = min(latest.values(), key=lambda p: (int(p['cost_cents']), p['profile_id']))
                cost += int(profile['cost_cents'])
                assignments.append(dict(window_id=wid, scenario_id=scenario['scenario_id'], shard_id=shard['shard_id'],
                                        profile_id=profile['profile_id'], selected_revision=profile['revision'],
                                        replica_id=profile['replica_id'], node_id='', demand_tokens_s=shard['demand_tokens_s'], cost_cents=profile['cost_cents']))
            decisions.append(dict(window_id=wid, scenario_id=scenario['scenario_id'], status='EXECUTABLE',
                                  total_cost_cents=cost, assigned_shards=len(workloads), required_shards=len(workloads),
                                  target_model=data['policies']['stable_model']))
    for name, rows in [('control_evidence.csv', evidence), ('recovery_options.csv', options),
                       ('recovery_assignments.csv', assignments), ('recovery_decisions.csv', decisions)]:
        write_csv(output / name, rows, list(rows[0]))
    return {w['window_id']: 'EXECUTABLE' for w in data['policies']['windows']}
