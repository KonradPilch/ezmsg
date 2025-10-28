import asyncio
import logging
import typing

from dataclasses import dataclass, field
from contextlib import contextmanager, suppress
from multiprocessing import resource_tracker
from multiprocessing.shared_memory import SharedMemory

from .netprotocol import (
    close_stream_writer,
    bytes_to_uint,
)

logger = logging.getLogger("ezmsg")

_std_register = resource_tracker.register


def _ignore_shm(name, rtype):
    if rtype == "shared_memory":
        return
    return resource_tracker._resource_tracker.register(self, name, rtype)  # noqa: F821


@contextmanager
def _untracked_shm() -> typing.Generator[None, None, None]:
    """
    Disable SHM tracking within context - https://bugs.python.org/issue38119.
    
    This context manager temporarily disables shared memory tracking to work
    around a Python bug where shared memory segments are not properly cleaned up.
    
    :return: Context manager generator.
    :rtype: typing.Generator[None, None, None]
    """
    resource_tracker.register = _ignore_shm
    yield
    resource_tracker.register = _std_register


class SHMContext:
    """
    SHMContext manages the memory map of a block of shared memory, and
    exposes memoryview objects for reading and writing.

    ezmsg shared memory format:
    [ UINT64 -- n_buffers ]
    [ UINT64 -- buf_size ]
    [ buf_size - 16 -- buf0 data_block ]
    ...
    [ 0x00 * 16 bytes -- reserved (header) ]
    [ buf_size - 16 -- buf1 data_block ]
    ...

    * n_buffers defines the number of shared memory buffers
    * buf_size defines the size of each shared memory buffer (including 16 bytes for header)
    * data_block is the remaining memory in this buffer which contains message information

    This format repeats itself for every buffer in the SharedMemory block.
    """

    _shm: SharedMemory
    _data_block_segs: typing.List[slice]

    num_buffers: int
    buf_size: int

    monitor: asyncio.Future

    def __init__(self, name: str) -> None:
        """
        Initialize SHMContext by connecting to an existing shared memory segment.
        
        :param name: The name of the shared memory segment to connect to.
        :type name: str
        :raises BufferError: If shared memory segment cannot be accessed.
        """
        with _untracked_shm():
            self._shm = SharedMemory(name=name, create=False)

        with self._shm.buf[0:8] as num_buffers_mem:
            self.num_buffers = bytes_to_uint(num_buffers_mem)

        with self._shm.buf[8:16] as buf_size_mem:
            self.buf_size = bytes_to_uint(buf_size_mem)

        buf_starts = [buf_idx * self.buf_size for buf_idx in range(self.num_buffers)]
        buf_stops = [buf_start + self.buf_size for buf_start in buf_starts]
        buf_data_block_starts = [buf_start + 16 for buf_start in buf_starts]

        self._data_block_segs = [
            slice(*seg) for seg in zip(buf_data_block_starts, buf_stops)
        ]

    @classmethod
    def _create(
        cls, shm_name: str, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> "SHMContext":
        """
        Create a new SHMContext with connection monitoring.
        
        :param shm_name: Name of the shared memory segment.
        :type shm_name: str
        :param reader: Stream reader for connection monitoring.
        :type reader: asyncio.StreamReader
        :param writer: Stream writer for connection cleanup.
        :type writer: asyncio.StreamWriter
        :return: New SHMContext instance with monitoring enabled.
        :rtype: SHMContext
        """
        context = cls(shm_name)

        async def monitor() -> None:
            try:
                await reader.read()
                logger.debug("Read from SHMContext monitor reader")
            except asyncio.CancelledError:
                pass
            finally:
                await close_stream_writer(writer)

        def close(_: asyncio.Future) -> None:
            context.close()

        context.monitor = asyncio.create_task(monitor(), name=f"{shm_name}_monitor")
        context.monitor.add_done_callback(close)
        return context

    @contextmanager
    def buffer(
        self, idx: int, readonly: bool = False
    ) -> typing.Generator[memoryview, None, None]:
        """
        Get a memory view of a specific buffer in the shared memory segment.
        
        :param idx: Index of the buffer to access.
        :type idx: int
        :param readonly: Whether to provide read-only access to the buffer.
        :type readonly: bool
        :return: Context manager yielding a memoryview of the buffer.
        :rtype: typing.Generator[memoryview, None, None]
        :raises BufferError: If shared memory is no longer accessible.
        """
        if self._shm.buf is None:
            raise BufferError(f"cannot access {self._shm.name}: server disconnected")

        with self._shm.buf[self._data_block_segs[idx]] as mem:
            if readonly:
                ro_mem = mem.toreadonly()
                yield ro_mem
                ro_mem.release()
            else:
                yield mem

    def close(self) -> None:
        """
        Close the shared memory context and cancel monitoring.
        
        This initiates an asynchronous close operation and cancels the
        connection monitor task.
        """
        asyncio.create_task(self.close_shm(), name=f"Close {self._shm.name}")
        self.monitor.cancel()

    async def close_shm(self) -> None:
        """
        Asynchronously close the shared memory segment.
        
        Retries closing if BufferError is encountered, as the segment
        may still be in use by other processes.
        """
        while True:
            try:
                self._shm.close()
                logger.debug("Closed SHM segment.")
                return
            except BufferError:
                logger.debug("BufferError caught... Sleeping.")
                await asyncio.sleep(1)

    async def wait_closed(self) -> None:
        """
        Wait for the shared memory context to be fully closed.
        
        This method waits for the monitoring task to complete, indicating
        that the connection has been properly terminated.
        """
        with suppress(asyncio.CancelledError):
            await self.monitor

    @property
    def name(self) -> str:
        """
        Get the name of the shared memory segment.
        
        :return: The shared memory segment name.
        :rtype: str
        """
        return self._shm.name

    @property
    def size(self) -> int:
        """
        Get the usable size of each buffer (excluding header).
        
        :return: Buffer size minus 16-byte header.
        :rtype: int
        """
        return self.buf_size - 16  # 16 byte header


@dataclass
class SHMInfo:
    """
    Information about a shared memory segment and its active leases.
    
    Tracks the SharedMemory object and manages client connection leases.
    When all leases are released, the shared memory is automatically cleaned up.
    """
    shm: SharedMemory
    leases: typing.Set["asyncio.Task[None]"] = field(default_factory=set)

    def lease(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> "asyncio.Task[None]":
        """
        Create a lease for this shared memory segment.
        
        The lease monitors the client connection and automatically releases
        the shared memory when the client disconnects.
        
        :param reader: Stream reader to monitor for client disconnection.
        :type reader: asyncio.StreamReader
        :param writer: Stream writer for connection cleanup.
        :type writer: asyncio.StreamWriter
        :return: Task representing the active lease.
        :rtype: asyncio.Task[None]
        """
        async def _wait_for_eof() -> None:
            try:
                await reader.read()
            finally:
                await close_stream_writer(writer)

        lease = asyncio.create_task(_wait_for_eof())
        lease.add_done_callback(self._release)
        self.leases.add(lease)
        return lease

    def _release(self, task: "asyncio.Task[None]"):
        self.leases.discard(task)
        logger.debug(f"discarded lease from {self.shm.name}")
        if len(self.leases) == 0:
            logger.debug(f"unlinking {self.shm.name}")
            self.shm.close()
            self.shm.unlink()
