import os
import asyncio
import typing
import logging

from uuid import UUID
from contextlib import contextmanager, suppress

from .shm import SHMContext
from .messagemarshal import MessageMarshal
from .backpressure import Backpressure

from .graphserver import GraphService
from .netprotocol import (
    Command,
    Address,
    AddressType, 
    read_str, 
    read_int, 
    uint64_to_bytes,
    encode_str,
    close_stream_writer,
    GRAPHSERVER_ADDR
)

logger = logging.getLogger("ezmsg")


NotificationQueue = asyncio.Queue[typing.Tuple[UUID, int]]


class CacheMiss(Exception): ...


class CacheEntry(typing.NamedTuple):
    object: typing.Any
    msg_id: int
    context: typing.ContextManager | None
    memory: memoryview | None


class MessageCache:

    _cache: list[CacheEntry | None]

    def __init__(self, num_buffers: int) -> None:
        self._cache = [None] * num_buffers

    def _buf_idx(self, msg_id: int) -> int:
        return msg_id % len(self._cache)

    def __getitem__(self, msg_id: int) -> typing.Any:
        """
        Get a cached object by msg_id
        
        :param msg_id: Message ID to retreive from cache
        :type msg_id: int
        :raises CacheMiss: If this msg_id does not exist in the cache.
        """
        entry = self._cache[self._buf_idx(msg_id)]
        if entry is None or entry.msg_id != msg_id:
            raise CacheMiss
        return entry.object
    
    def keys(self) -> list[int]:
        """
        Get a list of current cached msg_ids
        """
        return [entry.msg_id for entry in self._cache if entry is not None]
    
    def put_local(self, obj: typing.Any, msg_id: int) -> None:
        """
        Put an object with associated msg_id directly into cache
        
        :param obj: Object to put in cache.
        :type obj: typing.Any
        :param msg_id: ID associated with this message/object.
        :type msg_id: int
        """
        self._put(
            CacheEntry(
                object = obj,
                msg_id = msg_id,
                context = None,
                memory = None,
            )
        )

    def put_from_mem(self, mem: memoryview) -> None:
        """
        Reconstitute a message in mem and keep it in cache, releasing and
        overwriting the existing slot in cache.
        This method passes the lifecycle of the memoryview to the MessageCache
        and the memoryview will be properly released by the cache with `free`
        
        :param mem: Source memoryview containing serialized object.
        :type from_mem: memoryview
        :raises UninitializedMemory: If mem buffer is not properly initialized.
        """
        ctx = MessageMarshal.obj_from_mem(mem)
        self._put(
            CacheEntry(
                object = ctx.__enter__(),
                msg_id = MessageMarshal.msg_id(mem),
                context = ctx,
                memory = mem,
            )
        )

    def _put(self, entry: CacheEntry) -> None:
        buf_idx = self._buf_idx(entry.msg_id)
        self._release(buf_idx)
        self._cache[buf_idx] = entry

    def _release(self, buf_idx: int) -> None:
        entry = self._cache[buf_idx]
        if entry is not None:
            mem = entry.memory
            ctx = entry.context
            if ctx is not None:
                ctx.__exit__(None, None, None)
            del entry
            self._cache[buf_idx] = None
            if mem is not None:
                mem.release()
        
    def release(self, msg_id: int) -> None:
        """
        Release memory for the entry associated with msg_id
        
        :param mem: Source memoryview containing serialized object.
        :type from_mem: memoryview
        :raises UninitializedMemory: If mem buffer is not properly initialized.
        """
        buf_idx = self._buf_idx(msg_id)
        entry = self._cache[buf_idx]
        if entry is None or entry.msg_id != msg_id:
            raise CacheMiss
        self._release(buf_idx)

    def clear(self) -> None:
        """
        Release all cached objects
        
        :param mem: Source memoryview containing serialized object.
        :type from_mem: memoryview
        :raises UninitializedMemory: If mem buffer is not properly initialized.
        """
        for i in range(len(self._cache)):
            self._release(i)


class Channel:
    """cache-backed message channel for a particular publisher"""

    id: UUID
    pub_id: UUID
    pid: int
    topic: str

    num_buffers: int
    cache: MessageCache
    shm: SHMContext | None
    clients: dict[UUID, NotificationQueue | None]
    backpressure: Backpressure

    _graph_task: asyncio.Task[None]
    _pub_task: asyncio.Task[None]
    _pub_writer: asyncio.StreamWriter
    _graph_address: AddressType | None
    _local_backpressure: Backpressure | None

    def __init__(
        self, 
        id: UUID, 
        pub_id: UUID, 
        num_buffers: int, 
        shm: SHMContext | None,
        graph_address: AddressType | None,
    ) -> None:
        self.id = id
        self.pub_id = pub_id
        self.num_buffers = num_buffers
        self.shm = shm

        self.cache = MessageCache(self.num_buffers)
        self.backpressure = Backpressure(self.num_buffers)
        self.clients = dict()
        self._graph_address = graph_address
        self._local_backpressure = None

    @classmethod
    async def create(
        cls,
        pub_id: UUID,
        graph_address: AddressType,
    ) -> "Channel":
        graph_service = GraphService(graph_address)

        graph_reader, graph_writer = await graph_service.open_connection()
        graph_writer.write(Command.CHANNEL.value)
        graph_writer.write(encode_str(str(pub_id)))

        response = await graph_reader.read(1)
        if response != Command.COMPLETE.value:
            # FIXME: This will happen if the channel requested connection
            # to a non-existent (or non-publisher) UUID.  Ideally GraphServer
            # would tell us what happened rather than drop connection
            raise ValueError(f'failed to create channel {pub_id=}')
        
        id_str = await read_str(graph_reader)
        pub_address = await Address.from_stream(graph_reader)

        reader, writer = await asyncio.open_connection(*pub_address)
        writer.write(Command.CHANNEL.value)
        writer.write(encode_str(id_str))

        shm = None
        shm_name = await read_str(reader)
        try:
            shm = await graph_service.attach_shm(shm_name)
            writer.write(Command.SHM_OK.value)
        except (ValueError, OSError):
            shm = None
            writer.write(Command.SHM_ATTACH_FAILED.value)
        writer.write(uint64_to_bytes(os.getpid()))

        result = await reader.read(1)
        if result != Command.COMPLETE.value:
            # NOTE: The only reason this would happen is if the 
            # publisher's writer is closed due to a crash or shutdown
            raise ValueError(f'failed to create channel {pub_id=}')
        
        num_buffers = await read_int(reader)
        
        chan = cls(UUID(id_str), pub_id, num_buffers, shm, graph_address)

        chan._graph_task = asyncio.create_task(
            chan._graph_connection(graph_reader, graph_writer),
            name = f'chan-{chan.id}: _graph_connection'
        )

        chan._pub_writer = writer
        chan._pub_task = asyncio.create_task(
            chan._publisher_connection(reader),
            name = f'chan-{chan.id}: _publisher_connection'
        )

        logger.debug(f'created channel {chan.id=} {pub_id=} {pub_address=}')

        return chan
    
    def close(self) -> None:
        self._pub_task.cancel()
        self._graph_task.cancel()

    async def wait_closed(self) -> None:
        with suppress(asyncio.CancelledError):
            await self._pub_task
        with suppress(asyncio.CancelledError):
            await self._graph_task
        if self.shm is not None:
            await self.shm.wait_closed()
    
    async def _graph_connection(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        try:
            while True:
                cmd = await reader.read(1)
                
                if not cmd:
                    break

                else:
                    logger.warning(
                        f"Channel {self.id} rx unknown command from GraphServer: {cmd}"
                    )
        except (ConnectionResetError, BrokenPipeError):
            logger.debug(f"Channel {self.id} lost connection to graph server")

        finally:
            await close_stream_writer(writer)

    async def _publisher_connection(self, reader: asyncio.StreamReader) -> None:
        try:
            while True:
                msg = await reader.read(1)

                if not msg:
                    break

                msg_id = await read_int(reader)
                buf_idx = msg_id % self.num_buffers

                if msg == Command.TX_SHM.value:
                    shm_name = await read_str(reader)

                    if self.shm is not None and self.shm.name != shm_name:

                        shm_entries = self.cache.keys()
                        self.cache.clear()
                        self.shm.close()
                        await self.shm.wait_closed()

                        try:
                            self.shm = await GraphService(self._graph_address).attach_shm(shm_name)
                        except ValueError:
                            logger.info(
                                "Invalid SHM received from publisher; may be dead"
                            )
                            raise

                        for id in shm_entries:
                            self.cache.put_from_mem(self.shm[id % self.num_buffers])
                    
                    assert self.shm is not None
                    assert MessageMarshal.msg_id(self.shm[buf_idx]) == msg_id
                    self.cache.put_from_mem(self.shm[buf_idx])

                elif msg == Command.TX_TCP.value:
                    buf_size = await read_int(reader)
                    obj_bytes = await reader.readexactly(buf_size)
                    assert MessageMarshal.msg_id(obj_bytes) == msg_id
                    self.cache.put_from_mem(memoryview(obj_bytes).toreadonly())

                else:
                    raise ValueError(f"unimplemented data telemetry: {msg}")

                if not self._notify_clients(msg_id):
                    # Nobody is listening; need to ack!
                    self.cache.release(msg_id)
                    self._acknowledge(msg_id)

        except (ConnectionResetError, BrokenPipeError, asyncio.IncompleteReadError):
            logger.debug(f"connection fail: channel:{self.id} - pub:{self.pub_id}")

        finally:
            self.cache.clear()
            if self.shm is not None:
                self.shm.close()

            await close_stream_writer(self._pub_writer)

            logger.debug(f"disconnected: channel:{self.id} -> pub:{self.pub_id}")

    def _notify_clients(self, msg_id: int) -> bool:
        """ notify interested clients and return true if any were notified """
        buf_idx = msg_id % self.num_buffers
        for client_id, queue in self.clients.items():
            if queue is None: continue # queue is none if this is the pub
            self.backpressure.lease(client_id, buf_idx)
            queue.put_nowait((self.pub_id, msg_id))
        return not self.backpressure.available(buf_idx)

    def put_local(self, msg_id: int, msg: typing.Any) -> None:
        """
        put an object into cache (should only be used by Publishers)
        returns true if any clients were notified
        """
        if self._local_backpressure is None:
            raise ValueError('cannot put_local without access to publisher backpressure (is publisher in same process?)')
        
        buf_idx = msg_id % self.num_buffers
        if self._notify_clients(msg_id):
            self.cache.put_local(msg, msg_id)
            self._local_backpressure.lease(self.id, buf_idx)

    @contextmanager
    def get(self, msg_id: int, client_id: UUID) -> typing.Generator[typing.Any, None, None]:
        """
        Get a message
        
        :param msg_id: Message ID to retreive
        :type msg_id: int
        :param client_id: UUID of client retreiving this message for backpressure purposes
        :type client_id: UUID
        :raises CacheMiss: If this msg_id does not exist in the cache.
        """
        
        try:
            yield self.cache[msg_id]
        finally:
            buf_idx = msg_id % self.num_buffers
            self.backpressure.free(client_id, buf_idx)
            if self.backpressure.buffers[buf_idx].is_empty:
                self.cache.release(msg_id)

                # If pub is in same process as this channel, avoid TCP
                if self._local_backpressure is not None:
                    self._local_backpressure.free(self.id, buf_idx)
                else:
                    self._acknowledge(msg_id)

    def _acknowledge(self, msg_id: int) -> None:
        try:
            ack = Command.RX_ACK.value + uint64_to_bytes(msg_id)
            self._pub_writer.write(ack)
        except (BrokenPipeError, ConnectionResetError):
            logger.info(f"ack fail: channel:{self.id} -> pub:{self.pub_id}")

    def register_client(
        self, 
        client_id: UUID, 
        queue: NotificationQueue | None = None, 
        local_backpressure: Backpressure | None = None,
    ) -> None:
        self.clients[client_id] = queue
        if client_id == self.pub_id:
            self._local_backpressure = local_backpressure 

    def unregister_client(self, client_id: UUID) -> None:
        queue = self.clients[client_id]

        # queue is only 'None' if this client is a local publisher
        if queue is not None:
            for _ in range(queue.qsize()):
                pub_id, msg_id = queue.get_nowait()
                if pub_id != self.pub_id:
                    queue.put_nowait((pub_id, msg_id))

            self.backpressure.free(client_id)

        elif client_id == self.pub_id and self._local_backpressure is not None:
            self._local_backpressure.free(self.id)
            self._local_backpressure = None

        del self.clients[client_id]


def _ensure_address(address: AddressType | None) -> Address:
    if address is None:
        return Address.from_string(GRAPHSERVER_ADDR)

    elif not isinstance(address, Address):
        return Address(*address)
    
    return address


class ChannelManager:
    
    _registry: dict[Address, dict[UUID, Channel]]

    def __init__(self):
        default_address = Address.from_string(GRAPHSERVER_ADDR)
        self._registry = {default_address: dict()}

    async def register(
        self,
        pub_id: UUID,
        client_id: UUID,
        queue: NotificationQueue,
        graph_address: AddressType | None = None,
    ) -> Channel:
        return await self._register(pub_id, client_id, queue, graph_address, None)

    async def register_local_pub(
        self,
        pub_id: UUID,
        local_backpressure: Backpressure | None = None,
        graph_address: AddressType | None = None,
    ) -> Channel:
        return await self._register(pub_id, pub_id, None, graph_address, local_backpressure)

    async def _register(
        self, 
        pub_id: UUID, 
        client_id: UUID, 
        queue: NotificationQueue | None = None, 
        graph_address: AddressType | None = None,
        local_backpressure: Backpressure | None = None
    ) -> Channel:
        graph_address = _ensure_address(graph_address)
        try:
            channel = self._registry.get(graph_address, dict())[pub_id]
        except KeyError:
            channel = await Channel.create(pub_id, graph_address)
            channels = self._registry.get(graph_address, dict())
            channels[pub_id] = channel
            self._registry[graph_address] = channels
        channel.register_client(client_id, queue, local_backpressure)
        return channel

    async def unregister(
        self, 
        pub_id: UUID, 
        client_id: UUID, 
        graph_address: AddressType | None = None
    ) -> None:
        graph_address = _ensure_address(graph_address)
        channel = self._registry.get(graph_address, dict())[pub_id]
        channel.unregister_client(client_id)

        logger.debug(f'unregistered {client_id} from {pub_id}; {len(channel.clients)} left')

        if len(channel.clients) == 0:
            registry = self._registry[graph_address]
            del registry[pub_id]

            channel.close()
            await channel.wait_closed()

            logger.debug(f'closed channel {pub_id}: no clients')


CHANNELS = ChannelManager()