from asyncio import DatagramProtocol
import asyncio
from dataclasses import asdict, dataclass, is_dataclass
from datetime import datetime
from io import StringIO
import json
import logging
from socket import AF_INET
from typing import Any

from .tempest.messages import get_message_object

LOGGING_CONFIG = {}



class TempestProtocol(DatagramProtocol):
    log = logging.getLogger("tempest.udp")

    def datagram_received(self, data: bytes, addr) -> None:
        try:
            tempest_message = json.loads(str(data, encoding="utf-8"))
            self.handle_tempest_message(tempest_message, addr)
            # esle sssshhh
            return super().datagram_received(data, addr)
        except Exception:
            logging.exception("Error in handling datagram")

    def handle_tempest_message(self, tempest_message, addr):
        event_object = get_message_object(tempest_message)
        if event_object is not None:
            if is_dataclass(event_object):
                data = asdict(event_object)
                data.update({"type": event_object.__class__.__name__,
                            "src": addr[0]})
                print(json.dumps(data, default=lambda _: int(_.timestamp())))
            else:
                print("ERROR got non dataclass response: {}".format(event_object))


def main():
    logging.basicConfig(level=logging.WARNING)
    loop = asyncio.new_event_loop()
    async def init():
        log = logging.getLogger("main.async-init")
        log.info("Creating datagram listener.")
        ep = await asyncio.get_running_loop().create_datagram_endpoint(
            protocol_factory=lambda: TempestProtocol(),
            local_addr=("0.0.0.0", 50222),
            family=AF_INET,
        )
        log.info("listener created:{}".format(ep))

    loop.create_task(init())
    loop.run_forever()

if __name__ == '__main__':
    main()