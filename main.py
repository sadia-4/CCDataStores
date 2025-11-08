
from datacenters.datacenter import DataCenter
from client.client import ClientSession
async def main():
    dc1 = DataCenter('dc1')
    dc2 = DataCenter('dc2')
    dc1.peers.append(dc2)
    dc2.peers.append(dc1)
    # Start replication tasks
    asyncio.create_task(dc1.replicate_updates())
    asyncio.create_task(dc2.replicate_updates())
    # Run clients
    client1 = ClientSession(dc1)
    await client1.put('x','1')
    value = await client1.get('x')
    print(value)

if __name__ == '__main__':
    import random
    import asyncio
    asyncio.run(main())
