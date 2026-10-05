"""Rebuild an as-of operational handover from independent gateway exports."""
import argparse
import asyncio
import csv
import gzip
import json
from pathlib import Path
import tempfile
from fleetserve import Fleet
from fleetserve.telemetry.formats import normalize
from fleetserve.util import ManualClock


from .handoff_sources import read_csv, observations
from ..production.receipt_exports import gateway_index
from ..telemetry.correlations import bind

async def reconcile(root, output):
    spec = json.loads((root / 'incident.json').read_text(encoding='utf-8-sig'))
    gateways, receipts = gateway_index(read_csv(root / spec['gateway']), read_csv(root / spec['routes']), spec['cutoff'])
    links = read_csv(root / spec['bindings'])
    groups, errors = observations(root, spec['observations'])
    output.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix='handoff-') as state:
        fleet = Fleet.from_profile(root / spec['fleet_manifest'], state, clock=ManualClock(105))
        try:
            audit, ingestions = [], []
            for oid, versions in sorted(groups.items()):
                identity, reason = None, 'observation_conflict'
                if len(versions) == 1:
                    row = next(iter(versions.values()))
                    identity, reason = bind(row, links, gateways, spec['cutoff'])
                    if identity is not None:
                        source = fleet.config['telemetry']['producers'][row['producer']]
                        payload = dict(row['payload'])
                        payload['request' if source['format']=='export-v1' else 'request_id'] = identity
                        try:
                            normalize(payload, row['producer'], source)
                        except (ValueError, TypeError, KeyError):
                            identity, reason = None, 'invalid_payload'
                        except Exception as error:
                            from fleetserve.errors import FleetError
                            if not isinstance(error, FleetError):
                                raise
                            identity, reason = None, 'invalid_payload'
                        if identity is not None:
                            ingestions.append(dict(observation_id=oid, producer=row['producer'], record=payload))
                audit.append(dict(observation_id=oid, disposition='retained' if identity is not None else 'quarantined',
                                  reason=reason, admission_id=identity))
            for worker in fleet.config['demo_workers']:
                fleet.leases.register(**worker, ttl=10000)
            fleet.releases.bootstrap()
            fleet.outbox.reconcile()
            fleet.releases.begin('handoff', spec['alias'], spec['candidate'], 7500, 0, 'handoff.begin')
            fleet.outbox.reconcile()
            for receipt in receipts:
                fleet.evidence.receipt(receipt)
            for row in ingestions:
                fleet.evidence.ingest(row['producer'], [row['record']])
            for producer in fleet.config['telemetry']['required_sources']:
                for cohort in fleet.config['policy']['required_cohorts']:
                    fleet.evidence.watermark(producer, cohort, spec['window'][1])
            evaluated = fleet.assessor.evaluate('handoff', *spec['window'], 'handoff.assessment')
            metrics = {cohort:{side:{key:round(value,6) if isinstance(value,float) else value for key,value in stats.items()}
                              for side,stats in groups.items()} for cohort,groups in evaluated['metrics'].items()}
            outputs = {'receipts.json':receipts, 'ingestion.json':ingestions,
                       'reconciliation.json':dict(rows=audit,parse_errors=errors),
                       'assessment.json':dict(decision=evaluated['decision'],metrics=metrics,sample_ids=evaluated['sample_ids'])}
            for filename, value in outputs.items():
                (output / filename).write_text(json.dumps(value, ensure_ascii=False, indent=2)+'\n', encoding='utf-8')
        finally:
            await fleet.close()
