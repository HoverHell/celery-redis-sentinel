# -*- coding: utf-8 -*-
from __future__ import absolute_import, print_function, unicode_literals

try:
    import consulate
except ImportError:
    consulate = None

from kombu.transport.redis import Channel, Transport
from kombu.utils import cached_property

from .redis_sentinel import CelerySentinelConnectionPool, get_redis_via_sentinel


class SentinelChannel(Channel):
    """
    Redis Channel for interacting with Redis Sentinel

    .. note::
        In order to correctly configure the sentinel,
        this channel expects specific broker transport options to be
        provided via ``BROKER_TRANSPORT_OPTIONS``.
        Here is are sample transport options::

            BROKER_TRANSPORT_OPTIONS = {
                'sentinels': [('192.168.1.1', 26379),
                              ('192.168.1.2', 26379),
                              ('192.168.1.3', 26379)],
                'service_name': 'master',
                'socket_timeout': 0.1,
                # required kwargs
                'use_consul': True,
                'sentinel_port': 26379,
                'consul_ip_addr': 'localhost',
            }
    """
    from_transport_options = Channel.from_transport_options + (
        'sentinels',
        'service_name',
        'socket_timeout',
        'use_consul',
        'sentinel_port',
        'consul_ip_addr')

    # @cached_property
    @property
    def sentinel_pool(self):
        """
        # Cached property for getting connection pool to redis sentinel.
        Redis Sentinel addresses taken from Consul. So that it is a bad idea
        to cache this property. The addresses might change from time to time.

        In addition to returning connection pool, this property
        changes the ``Transport`` connection details to match the
        connected master so that celery can correctly log to which
        node it is actually connected.

        Returns
        -------
        CelerySentinelConnectionPool
            Connection pool instance connected to redis sentinel
        """
        params = self._connparams()
        params.update({
            'service_name': getattr(self, 'service_name', 'mymaster'),
            'socket_timeout': getattr(self, 'socket_timeout', 0.1),
        })

        if getattr(self, 'use_consul', True) and consulate is not None:
            consul = consulate.Consul(host=getattr(self, 'consul_ip_addr', 'localhost'))
            params['sentinels'] = [
                (node['Address'], getattr(self, 'sentinel_port', 6378)) for node in consul.catalog.nodes() 
                if node['Meta'].get('consul_role') == 'server'
            ]
        else:
            params['sentinels'] = getattr(self, 'sentinels', [])

        sentinel = get_redis_via_sentinel(
            redis_class=self.Client,
            connection_pool_class=CelerySentinelConnectionPool,
            **params
        )
        pool = sentinel.connection_pool
        hostname, port = pool.get_master_address()

        # update connection details so that celery correctly logs
        # where it connects
        self.connection.client.hostname = hostname
        self.connection.client.port = port

        return pool

    def _get_pool(self, *args, **kwargs):
        return self.sentinel_pool


class SentinelTransport(Transport):
    """
    Redis transport with support for Redis Sentinel.
    """
    Channel = SentinelChannel
