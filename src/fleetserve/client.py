"""A standard-library operations client; server-side errors retain machine codes."""
import json
import urllib.error
import urllib.request
from .errors import FleetError


class Client:
    def __init__(self,base_url,token,timeout=30):
        self.base_url,self.token,self.timeout = base_url.rstrip('/'),token,timeout

    def request(self,method,path,payload=None,key=None):
        data = json.dumps(payload).encode() if payload is not None else None
        headers = {'Authorization':'Bearer '+self.token,'Content-Type':'application/json'}
        if key is not None:
            headers['Idempotency-Key'] = key
        request = urllib.request.Request(self.base_url+path,data=data,headers=headers,method=method)
        try:
            with urllib.request.urlopen(request,timeout=self.timeout) as response:
                return json.load(response)
        except urllib.error.HTTPError as error:
            value = json.load(error).get('error',{})
            raise FleetError(value.get('code','http_error'),value.get('message',''),error.code) from None

    def generate(self,key,model,prompt,max_tokens=8):
        return self.request('POST','/v1/generate',{'model':model,'prompt':prompt,'max_tokens':max_tokens},key)

    def release(self,release_id,alias,revision,candidate_bps,expected_epoch,operation_id):
        return self.request('POST','/admin/releases',dict(release_id=release_id,alias=alias,revision=revision,
                           candidate_bps=candidate_bps,expected_epoch=expected_epoch,operation_id=operation_id))

    def evaluate(self,release_id,start,end,assessment_id):
        return self.request('POST','/admin/evaluate',dict(release_id=release_id,start=start,end=end,assessment_id=assessment_id))

    def apply(self,assessment_id,operation_id):
        return self.request('POST','/admin/apply',dict(assessment_id=assessment_id,operation_id=operation_id))

    def reconcile(self):
        return self.request('POST','/admin/reconcile',{})
