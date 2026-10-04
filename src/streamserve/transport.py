"""Small HTTP/1.1 ASGI host for local deployments and wire integration tests.

One request per connection; responses without content-length use chunked
encoding. Request bodies require content-length. This intentionally narrow
transport makes lifecycle tests runnable with only the Python standard library.
"""
import asyncio
from urllib.parse import urlsplit, unquote


class HTTPServer:
    def __init__(self, application, host='127.0.0.1', port=0):
        self.application = application
        self.host, self.port = host, port
        self.server = None
        self.connections = set()

    async def start(self):
        self.server = await asyncio.start_server(self._connection, self.host, self.port, limit=65536)
        self.port = self.server.sockets[0].getsockname()[1]
        return self

    async def _connection(self, reader, writer):
        current = asyncio.current_task()
        self.connections.add(current)
        try:
            await self._request(reader, writer)
        except (ConnectionError, asyncio.IncompleteReadError, asyncio.LimitOverrunError, ValueError):
            pass
        finally:
            writer.close()
            try:
                await writer.wait_closed()
            except ConnectionError:
                pass
            finally:
                self.connections.discard(current)

    async def _request(self, reader, writer):
        head = await reader.readuntil(b'\r\n\r\n')
        lines = head[:-4].split(b'\r\n')
        method, target, version = lines[0].decode('ascii').split(' ')
        if version != 'HTTP/1.1':
            raise ValueError('HTTP/1.1 required')
        headers = []
        mapping = {}
        for line in lines[1:]:
            name, value = line.split(b':', 1)
            key = name.strip().lower()
            if key in mapping:
                raise ValueError('duplicate header')
            mapping[key] = value.strip()
            headers.append((key, value.strip()))
        if b'transfer-encoding' in mapping:
            raise ValueError('chunked requests unsupported')
        length = int(mapping.get(b'content-length', b'0'))
        if length < 0 or length > self.application.engine.settings.max_body_bytes:
            writer.write(b'HTTP/1.1 413 Payload Too Large\r\nContent-Length: 0\r\nConnection: close\r\n\r\n')
            await writer.drain()
            return
        body = await reader.readexactly(length)
        parsed = urlsplit(target)
        delivered = False
        started = False
        chunked = False

        async def receive():
            nonlocal delivered
            if not delivered:
                delivered = True
                return {'type': 'http.request', 'body': body, 'more_body': False}
            await reader.read()
            return {'type': 'http.disconnect'}

        async def send(message):
            nonlocal started, chunked
            if message['type'] == 'http.response.start':
                if started:
                    raise ValueError('headers sent twice')
                started = True
                response_headers = message.get('headers', [])
                chunked = not any(k.lower() == b'content-length' for k, _ in response_headers)
                writer.write(f"HTTP/1.1 {message['status']} Response\r\n".encode())
                for key, value in response_headers:
                    writer.write(key + b': ' + value + b'\r\n')
                if chunked:
                    writer.write(b'Transfer-Encoding: chunked\r\n')
                writer.write(b'Connection: close\r\n\r\n')
            elif message['type'] == 'http.response.body':
                data = message.get('body', b'')
                if data:
                    writer.write((f'{len(data):x}\r\n'.encode() + data + b'\r\n') if chunked else data)
                if chunked and not message.get('more_body', False):
                    writer.write(b'0\r\n\r\n')
            await writer.drain()

        scope = {'type': 'http', 'method': method, 'path': unquote(parsed.path),
                 'query_string': parsed.query.encode(), 'headers': headers, 'http_version': '1.1'}
        await self.application(scope, receive, send)

    async def close(self):
        if self.server:
            self.server.close()
            await self.server.wait_closed()
        for task in list(self.connections):
            task.cancel()
        await asyncio.gather(*list(self.connections), return_exceptions=True)
