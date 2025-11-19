import logging

from uuid import UUID

from .messagechannel import Channel, NotificationQueue
from .backpressure import Backpressure
from .netprotocol import (
    Address,
    AddressType, 
    GRAPHSERVER_ADDR
)

logger = logging.getLogger("ezmsg")

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
