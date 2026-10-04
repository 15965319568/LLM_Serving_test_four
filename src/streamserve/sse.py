"""SSE framing and an incremental UTF-8 client decoder."""
import codecs
import json


def frame(event):
    value = json.dumps(event.data, ensure_ascii=False, separators=(',', ':'))
    return f'id: {event.seq}\nevent: {event.kind}\ndata: {value}\n\n'.encode('utf-8')


class Decoder:
    def __init__(self):
        self.decoder = codecs.getincrementaldecoder('utf-8')()
        self.buffer = ''
        self.lines = []

    def feed(self, chunk, final=False):
        self.buffer += self.decoder.decode(chunk, final=final)
        result = []
        while '\n' in self.buffer:
            line, self.buffer = self.buffer.split('\n', 1)
            line = line.rstrip('\r')
            if line:
                self.lines.append(line)
                continue
            fields = {'data': []}
            for item in self.lines:
                if item.startswith(':'):
                    continue
                name, sep, value = item.partition(':')
                if value.startswith(' '):
                    value = value[1:]
                if name == 'data':
                    fields['data'].append(value)
                elif name in ('id', 'event'):
                    fields[name] = value
            self.lines.clear()
            if fields['data']:
                fields['data'] = '\n'.join(fields['data'])
                result.append(fields)
        return result
