import asyncio
from rqueue.schemas import Observable


class Healthchecker:
    def __init__(self, port: int, app: Observable):
        self.port = port
        self.app = app

    async def run(self):
        server = await asyncio.start_server(self._handle, "0.0.0.0", self.port)

        async with server:
            await server.serve_forever()

    async def _handle(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        try:
            while True:
                line = await reader.readline()
                if not line.strip():
                    break

            if not self.app:
                raise RuntimeError("observable object has not been provided")

            s = self.app.status()

            if not s.ok:
                raise RuntimeError("observable object is unhealthy")

            body = s.model_dump_json().encode()

            response = b"\r\n".join(
                [
                    b"HTTP/1.1 200 OK",
                    b"Content-Type: application/json",
                    b"Content-Length: " + str(len(body)).encode(),
                    b"Connection: close",
                    b"",
                    body,
                ]
            )

            writer.write(response)
            await writer.drain()
        finally:
            writer.close()
            await writer.wait_closed()
