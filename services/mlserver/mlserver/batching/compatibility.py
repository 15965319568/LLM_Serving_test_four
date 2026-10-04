"""Wire-level compatibility for independent requests sharing one model call."""
import json
import math
from ..types import InferenceRequest


def validate_request(request: InferenceRequest):
    names = set()
    rows = None
    for head in request.inputs:
        if head.name in names or not head.shape or any(type(n) is not int or n <= 0 for n in head.shape):
            raise ValueError('invalid input names or dimensions')
        names.add(head.name)
        if rows is not None and rows != head.shape[0]:
            raise ValueError('input heads disagree on minibatch size')
        rows = head.shape[0]
        data = getattr(head.data, 'root', head.data)
        if isinstance(data, list) and len(data) != math.prod(head.shape):
            raise ValueError('input length does not match shape')
    if rows is None:
        raise ValueError('at least one input is required')
    return rows


def compatibility_key(request: InferenceRequest):
    validate_request(request)
    inputs = sorted((head.name, head.datatype, head.shape[1:],
                     head.parameters.model_dump() if head.parameters else None) for head in request.inputs)
    outputs = None if request.outputs is None else sorted(
        (head.name, head.parameters.model_dump() if head.parameters else None) for head in request.outputs)
    parameters = request.parameters.model_dump() if request.parameters else None
    if parameters and parameters.get('headers'):
        parameters['headers'] = {key: value for key, value in parameters['headers'].items()
                                 if key.lower() not in ('ce-id', 'ce-requestid')}
    return json.dumps([inputs, outputs, parameters], sort_keys=True, separators=(',', ':'), allow_nan=False)


def validate_response(response, total_rows):
    names = set()
    if not response.outputs:
        raise ValueError('batch response has no outputs')
    for output in response.outputs:
        if output.name in names or not output.shape or output.shape[0] != total_rows:
            raise ValueError('batch response row count or output names are invalid')
        names.add(output.name)
        if any(type(n) is not int or n <= 0 for n in output.shape):
            raise ValueError('invalid response shape')
        data = getattr(output.data, 'root', output.data)
        if isinstance(data, list) and len(data) != math.prod(output.shape):
            raise ValueError('response length does not match shape')
