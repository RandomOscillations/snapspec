import asyncio
import base64

from snapspec.network.protocol import MessageType, encode_message, read_message


HOST = "10.40.129.153"  # Replace with the worker VM's internal VCL IP.
PORT = 9000


async def send(msg_type, ts, **kwargs):
    reader, writer = await asyncio.open_connection(HOST, PORT)
    writer.write(encode_message(msg_type, ts, **kwargs))
    await writer.drain()
    resp = await read_message(reader)
    writer.close()
    await writer.wait_closed()
    return resp


async def main():
    payload = base64.b64encode(b"hello".ljust(4096, b"\x00")).decode("ascii")

    resp = await send(
        MessageType.WRITE,
        1,
        block_id=0,
        data=payload,
        dep_tag=0,
        role="NONE",
        partner=-1,
    )
    print("WRITE:", resp)

    resp = await send(MessageType.READ, 2, block_id=0)
    print("READ:", resp)

    resp = await send(MessageType.PAUSE, 3)
    print("PAUSE:", resp)

    resp = await send(MessageType.SNAP_NOW, 4)
    print("SNAP_NOW:", resp)

    resp = await send(MessageType.COMMIT, 5, snapshot_id=1)
    print("COMMIT:", resp)

    resp = await send(MessageType.RESUME, 6)
    print("RESUME:", resp)


if __name__ == "__main__":
    asyncio.run(main())
