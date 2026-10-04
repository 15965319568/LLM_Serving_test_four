"""Capture a local synthetic release exercise without a golden-answer table."""
import argparse
import asyncio
import csv
import gzip
import hashlib
import json
from pathlib import Path
import random
import tempfile
from mlserver.types import InferenceRequest,RequestInput,Parameters
from .. import Fleet
from ..inference import ServingFabric
from ..util import ManualClock
from . import ProductionService,DeliveryInbox


async def capture(output,manifest=Path('config/fleet/manifest.json'),seed=714):
    output=Path(output);output.mkdir(parents=True,exist_ok=True)
    if (output/'capture.json').exists():raise ValueError('Choose a new capture output directory')
    raw=output/'raw';raw.mkdir(exist_ok=True)
    numeric=json.loads(Path('config/production-numeric.json').read_text())
    rng=random.Random(seed);clock=ManualClock(100)
    observations=[];quality=[];envelopes={p:[] for p in numeric['partitions']}
    def note(kind,**values):observations.append(dict(seq=len(observations),clock=clock(),kind=kind,**values))
    def envelope(partition,kind,body,identity=None):
        row=dict(schema='delivery/1',offset=len(envelopes[partition]),kind=kind,body=body)
        if identity is not None:row['admission_id']=identity
        envelopes[partition].append(row)
    def key(tenant,candidate,prefix):
        for i in range(10000):
            value=prefix+str(i)
            bucket=int.from_bytes(hashlib.sha256(('\0'.join(['exercise',tenant,value])).encode()).digest()[:8],'big')%10000
            if (bucket<2000)==candidate:return value
    with tempfile.TemporaryDirectory(prefix='production-replay-') as state:
        fleet=Fleet.from_profile(manifest,Path(state)/'fleet',clock=clock)
        service=ProductionService(fleet,ServingFabric(Path(state)/'fabric',workers=2))
        inbox=None
        try:
            await service.start();inbox=DeliveryInbox(fleet,numeric['partitions'])
            for spec in fleet.config['demo_workers']:
                worker=fleet.leases.register(**spec,ttl=10000)
                await service.install(worker['worker'],worker['generation'],numeric['revision_factors'][worker['revision']],
                    'install.'+worker['worker'],max_batch_size=8,max_batch_time=.03,runtime_options={'events':str((raw/'workers').resolve())})
            note('initialized',state=fleet.snapshot())
            note('release',result=fleet.releases.begin('exercise','chat','r20261004',2000,0,'exercise.begin'))
            fleet.outbox.reconcile();note('published',state=fleet.snapshot())
            for window in range(2):
                clock.now=105+window*40
                calls=[]
                for tenant in ['north','south']:
                    for candidate in [False,True]:
                        for index in range(5):
                            values=[float(index+1),float(index+2)] if index%2 else [float(index+1)]
                            req=InferenceRequest(id='client-'+str(index),parameters=Parameters(offset=0),
                                  inputs=[RequestInput(name='x',datatype='FP64',shape=[len(values),1],data=values)])
                            original_key=key(tenant,candidate,f'{tenant}.{window}.{index}.')
                            calls.append((tenant,original_key,req))
                tasks=[asyncio.create_task(service.infer(tenant,'chat',k,req)) for tenant,k,req in calls]
                results=await asyncio.gather(*tasks,return_exceptions=True)
                for (tenant,k,req),result in zip(calls,results):
                    note('client',tenant=tenant,key=k,request=req.model_dump(mode='json'),
                         response=result.model_dump(mode='json') if not isinstance(result,BaseException) else None,
                         error=type(result).__name__ if isinstance(result,BaseException) else None)
                    if not isinstance(result,BaseException):
                        target=[2*v for v in req.inputs[0].data.root]
                        values=result.outputs[0].data.root
                        error=1 if len(values)!=len(target) else sum(abs(a-b)/max(1,abs(b)) for a,b in zip(values,target))/len(target)
                        quality.append(dict(admission_id=result.parameters.admission_id,score=max(0,1-error),finished_at=220+window*40))
                note('window',window=window,accounting=service.accounting())
            admissions=service.ledger.rows()
            terminals=service.terminal_records()
            (raw/'client.jsonl').write_text(''.join(json.dumps(r,ensure_ascii=False)+'\n' for r in observations),encoding='utf-8')
            (raw/'terminal.jsonl').write_text(''.join(json.dumps(r)+'\n' for r in terminals),encoding='utf-8')
            (raw/'quality-review.jsonl').write_text(''.join(json.dumps(r)+'\n' for r in quality),encoding='utf-8')
            fields=['request_id','tenant','alias','revision','cohort','admitted','kind','source']
            receipts=[dict(request_id=row['id'],tenant=row['tenant'],alias=row['alias'],revision=row['revision'],
                cohort=row['cohort'],admitted=row['admitted'],kind='live',source='gateway') for row in admissions]
            with (raw/'admissions.csv').open('w',encoding='utf-8-sig',newline='') as stream:
                writer=csv.DictWriter(stream,fieldnames=fields);writer.writeheader();writer.writerows(receipts)
            byid={row['id']:row for row in admissions}
            for index,terminal in enumerate(terminals):
                admission=byid[terminal['request_id']];p='edge-east' if admission['tenant']=='north' else 'edge-west'
                converted=dict(terminal)
                unit=['ms','s','us'][index%3];converted['latency_unit']=unit
                converted['latency']=terminal['latency']*{'ms':1,'s':.001,'us':1000}[unit]
                envelope(p,'event',converted,admission['id'])
                old=dict(schema=1,id='archive.'+admission['id'],request=admission['external_id'],finished_ms=terminal['finished_at']*1000,
                    duration_ms=terminal['latency'],status={'completed':'ok','failed':'error','cancelled':'cancel'}[terminal['outcome']],model='chat')
                envelope('legacy-export','event',old,admission['id'])
                advisory=dict(terminal,producer='edge',rank=9999,sample_kind='shadow')
                envelope('worker-observations','event',advisory,admission['id'])
            for review in quality:
                identity=review['admission_id']
                base=dict(schema=2,event_id='quality.'+identity,request_id=byid[identity]['external_id'],finished_at=review['finished_at'])
                # Keep provisional export, reviewer correction and a later
                # delivery of the old export as separate original observations.
                envelope('quality-backfill','event',dict(base,update_seq=0,score=1),identity)
                envelope('quality-backfill','event',dict(base,update_seq=2,score=review['score']),identity)
                envelope('quality-backfill','event',dict(base,update_seq=0,score=1),identity)
            if receipts:
                malformed=dict(schema=2,event_id='malformed',update_seq=0,finished_at=160,latency=-1,outcome='completed')
                envelope('edge-east','event',malformed,receipts[0]['request_id'])
            for p in envelopes:
                for cohort in ['interactive','batch']:
                    envelope(p,'watermark',dict(cohort=cohort,through=180))
            files=[];transforms=[]
            for p,records in envelopes.items():
                # The transport export is deliberately separate from originals.
                copies=records+records[::7];rng.shuffle(copies)
                for part in range(2):
                    subset=copies[part::2]
                    directory=output/'deliveries';directory.mkdir(exist_ok=True)
                    if p=='legacy-export':
                        path=directory/f'{p}.{part}.csv'
                        with path.open('w',encoding='utf-8-sig',newline='') as stream:
                            writer=csv.DictWriter(stream,fieldnames=['schema','offset','kind','admission_id','body']);writer.writeheader()
                            for row in subset:writer.writerow(dict(row,body=json.dumps(row['body'])))
                        fmt='csv'
                    else:
                        path=directory/f'{p}.{part}.jsonl.gz'
                        with gzip.open(path,'wt',encoding='utf-8-sig') as stream:
                            for i,row in enumerate(subset):
                                stream.write(json.dumps(row)+'\n')
                                if i==2:stream.write('{"schema":"delivery/1","offset":\n\n')
                        fmt='jsonl'
                    files.append(dict(partition=p,path=path.relative_to(output).as_posix(),format=fmt))
                transforms.append(dict(partition=p,original_envelopes=len(records),deliveries=len(copies)))
            rng.shuffle(files)
            (output/'capture.json').write_text(json.dumps(dict(files=files),indent=2),encoding='utf-8')
            (output/'partitions.json').write_text(json.dumps(numeric['partitions'],indent=2),encoding='utf-8')
            imported=inbox.import_manifest(output/'capture.json')
            note('collector',result=imported,partitions=inbox.status())
            try:note('assessment',result=fleet.assessor.evaluate('exercise',100,180,'capture.assessment'))
            except Exception as error:note('assessment_error',error=str(error))
            (raw/'controller.jsonl').write_text(''.join(json.dumps(r,ensure_ascii=False)+'\n' for r in observations if r['kind']!='client'),encoding='utf-8')
            hashes={p.relative_to(output).as_posix():hashlib.sha256(p.read_bytes()).hexdigest() for p in sorted(output.rglob('*')) if p.is_file()}
            (output/'provenance.json').write_text(json.dumps(dict(origin='synthetic actual CPU execution',seed=seed,
                source='fleetserve.production.replay',transport=transforms,sha256=hashes,
                quality='CPU exercise: one minus mean relative absolute error against x*2; provisional export and reviewer revision coexist'),indent=2),encoding='utf-8')
        finally:
            if inbox:inbox.close()
            await service.close()
    provenance_path=output/'provenance.json'
    provenance=json.loads(provenance_path.read_text(encoding='utf-8'))
    provenance['sha256']={p.relative_to(output).as_posix():hashlib.sha256(p.read_bytes()).hexdigest()
        for p in sorted(output.rglob('*')) if p.is_file() and p!=provenance_path}
    provenance_path.write_text(json.dumps(provenance,indent=2),encoding='utf-8')
    return dict(output=str(output),admissions=len(admissions),quality_reviews=len(quality),delivery_records=sum(t['deliveries'] for t in transforms))


def main():
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--output',type=Path,required=True)
    parser.add_argument('--manifest',type=Path,default=Path('config/fleet/manifest.json'));parser.add_argument('--seed',type=int,default=714)
    args=parser.parse_args();print(json.dumps(asyncio.run(capture(args.output,args.manifest,args.seed))))


if __name__=='__main__':main()
