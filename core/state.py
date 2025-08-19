import asyncio
from core.queue_manager import ImportQueues

queues = ImportQueues()
asyncio.run(queues.start())
