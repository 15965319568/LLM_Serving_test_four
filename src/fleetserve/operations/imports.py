import csv
import gzip
import json
from pathlib import Path
from ..errors import FleetError


def json_records(path):
    path = Path(path)
    opener = gzip.open if path.suffix == '.gz' else open
    with opener(path,'rt',encoding='utf-8-sig') as stream:
        for line_number,line in enumerate(stream,1):
            if not line.strip():
                continue
            try:
                value = json.loads(line)
            except json.JSONDecodeError:
                yield line_number, None
            else:
                yield line_number, value


def import_events(evidence, producer, path, batch_size=256):
    if type(batch_size) is not int or not 1 <= batch_size <= 10000:
        raise FleetError('invalid_batch_size')
    report = {'inserted':0,'updated':0,'duplicates':0,'older':0,'conflicts':0,'rejected':[]}
    batch, lines = [], []

    def flush():
        if not batch:
            return
        result = evidence.ingest(producer,batch)
        for key in ('inserted','updated','duplicates','older','conflicts'):
            report[key] += result[key]
        report['rejected'].extend({'line':lines[e['index']],'code':e['code']} for e in result['rejected'])
        batch.clear(); lines.clear()

    for line_number,value in json_records(path):
        if value is None:
            report['rejected'].append({'line':line_number,'code':'invalid_json'})
            continue
        batch.append(value); lines.append(line_number)
        if len(batch) >= batch_size:
            flush()
    flush()
    report['evidence_version'] = evidence.version()
    return report


def import_receipts(evidence,path):
    report = {'inserted':0,'duplicates':0,'rejected':[]}
    with open(path,newline='',encoding='utf-8-sig') as stream:
        for number,row in enumerate(csv.DictReader(stream),2):
            try:
                row['admitted'] = float(row['admitted'])
                inserted = evidence.receipt(row)
            except (FleetError,KeyError,TypeError,ValueError) as error:
                report['rejected'].append({'line':number,'code':getattr(error,'code','invalid_receipt')})
            else:
                report['inserted' if inserted else 'duplicates'] += 1
    return report


def import_bundle(fleet, manifest_path):
    path = Path(manifest_path)
    manifest = json.loads(path.read_text(encoding='utf-8'))
    result = {'receipts':import_receipts(fleet.evidence,path.parent/manifest['receipts']), 'sources':[]}
    for source in manifest['sources']:
        report = import_events(fleet.evidence,source['producer'],path.parent/source['file'])
        result['sources'].append({'producer':source['producer'],'file':source['file'],**report})
    for mark in manifest.get('watermarks',[]):
        fleet.evidence.watermark(mark['producer'],mark['cohort'],mark['through'])
    return result
