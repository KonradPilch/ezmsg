import asyncio
import logging
import typing

from .graphserver import GraphServer, GraphService
from .pubclient import Publisher
from .subclient import Subscriber

from types import TracebackType

logger = logging.getLogger("ezmsg")


class GraphContext:
    """
    GraphContext maintains a list of created publishers, subscribers, and connections in the graph.
    
    The GraphContext provides a managed environment for creating and tracking publishers,
    subscribers, and graph connections. When the context is no longer needed, it can
    revert changes in the graph which disconnects publishers and removes modifications
    that this context made.
    
    It also maintains a context manager that ensures the GraphServer is running.

    :param graph_service: Optional graph service instance to use
    :type graph_service: typing.Optional[GraphService]

    .. note::
    The GraphContext is typically managed automatically by the ezmsg runtime
    and doesn't need to be instantiated directly by user code.
    """

    _clients: typing.Set[typing.Union[Publisher, Subscriber]]
    _edges: typing.Set[typing.Tuple[str, str]]

    _graph_service: GraphService
    _graph_server: typing.Optional[GraphServer]

    def __init__(
        self,
        graph_service: typing.Optional[GraphService] = None,
    ) -> None:
        self._clients = set()
        self._edges = set()
        self._graph_service = (
            graph_service if graph_service is not None else GraphService()
        )
        self._graph_server = None

    async def publisher(self, topic: str, **kwargs) -> Publisher:
        """
        Create a publisher for the specified topic.
        
        :param topic: The topic name to publish to
        :type topic: str
        :param kwargs: Additional keyword arguments for publisher configuration
        :return: A Publisher instance for the topic
        :rtype: Publisher
        """
        pub = await Publisher.create(topic, self._graph_service, **kwargs)
        self._clients.add(pub)
        return pub

    async def subscriber(self, topic: str, **kwargs) -> Subscriber:
        """
        Create a subscriber for the specified topic.
        
        :param topic: The topic name to subscribe to
        :type topic: str
        :param kwargs: Additional keyword arguments for subscriber configuration
        :return: A Subscriber instance for the topic
        :rtype: Subscriber
        """
        sub = await Subscriber.create(topic, self._graph_service, **kwargs)
        self._clients.add(sub)
        return sub

    async def connect(self, from_topic: str, to_topic: str) -> None:
        """
        Connect two topics in the message graph.
        
        :param from_topic: The source topic name
        :type from_topic: str
        :param to_topic: The destination topic name
        :type to_topic: str
        """
        await self._graph_service.connect(from_topic, to_topic)
        self._edges.add((from_topic, to_topic))

    async def disconnect(self, from_topic: str, to_topic: str) -> None:
        """
        Disconnect two topics in the message graph.
        
        :param from_topic: The source topic name
        :type from_topic: str
        :param to_topic: The destination topic name
        :type to_topic: str
        """
        await self._graph_service.disconnect(from_topic, to_topic)
        self._edges.discard((from_topic, to_topic))

    async def sync(self, timeout: typing.Optional[float] = None) -> None:
        """
        Synchronize with the graph server.
        
        :param timeout: Optional timeout for the sync operation
        :type timeout: typing.Optional[float]
        """
        await self._graph_service.sync(timeout)

    async def pause(self) -> None:
        """
        Pause message processing in the graph.
        """
        await self._graph_service.pause()

    async def resume(self) -> None:
        """
        Resume message processing in the graph.
        """
        await self._graph_service.resume()

    async def _ensure_servers(self) -> None:
        self._graph_server = await self._graph_service.ensure()

    async def _shutdown_servers(self) -> None:
        if self._graph_server is not None:
            self._graph_server.stop()
        self._graph_server = None

    async def __aenter__(self) -> "GraphContext":
        await self._ensure_servers()
        return self

    async def __aexit__(
        self,
        exc_t: typing.Optional[typing.Type[Exception]],
        exc_v: typing.Optional[typing.Any],
        exc_tb: typing.Optional[TracebackType],
    ) -> bool:
        await self.revert()
        await self._shutdown_servers()
        return False

    async def revert(self) -> None:
        """
        Revert all changes made by this context.
        
        This method closes all clients (publishers and subscribers) created by this
        context and removes all edges that were added to the graph. It is
        automatically called when exiting the context manager.
        """
        for client in self._clients:
            client.close()

        wait = [c.wait_closed() for c in self._clients]
        for future in asyncio.as_completed(wait):
            await future

        for edge in self._edges:
            try:
                await self._graph_service.disconnect(*edge)
            except (ConnectionRefusedError, BrokenPipeError, ConnectionResetError) as e:
                logger.warn(f"Could not remove edge {edge} from GraphServer: {e}")
