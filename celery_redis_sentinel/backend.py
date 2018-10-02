# -*- coding: utf-8 -*-
from __future__ import absolute_import, print_function, unicode_literals

try:
    import consulate
except ImportError:
    consulate = None

from celery.backends.redis import RedisBackend
from kombu.utils import cached_property
import celery

from .redis_sentinel import EnsuredRedisMixin, get_redis_via_sentinel

try:
    if celery.VERSION.major >= 4:
        from redis import StrictRedis as Redis
    else:
        from redis import Redis
except AttributeError:
    from redis import Redis


class RedisSentinelBackend(RedisBackend):
    """
    Redis results backend with support for Redis Sentinel

    .. note::
        In order to correctly configure the sentinel,
        this backend expects an additional backend celery
        configuration to be present - ``CELERY_RESULT_BACKEND_TRANSPORT_OPTIONS``.
        Here is are sample transport options::

            CELERY_RESULT_BACKEND_TRANSPORT_OPTIONS = {
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

    def __init__(self, transport_options=None, *args, **kwargs):
        super(RedisSentinelBackend, self).__init__(*args, **kwargs)

        _get = self.app.conf.get
        self.transport_options = transport_options or _get('CELERY_RESULT_BACKEND_TRANSPORT_OPTIONS') or {}
        self.sentinels = self.transport_options['sentinels']
        self.service_name = self.transport_options['service_name']
        self.socket_timeout = self.transport_options.get('socket_timeout', 0.1)
        self.use_consul = self.transport_options.get('use_consul', False)
        self.sentinel_port = self.transport_options.get('sentinel_port', 26379)
        self.consul_ip_addr = self.transport_options.get('consul_ip_addr', 'localhost')

    # @cached_property
    def client(self):
        """
        #Cached property for getting ``Redis`` client to be used to interact with redis.
        Not sure if it is ok to cache client, because 

        Returned client also subclasses from :class:`.EnsuredRedisMixin` which
        ensures that all redis commands are executed with retry logic in case
        of sentinel failover.

        Returns
        -------
        Redis
            Redis client connected to Sentinel via Sentinel connection pool
        """
        params = self.connparams
        params.update({
            'service_name': self.service_name,
            'socket_timeout': self.socket_timeout,
        })
        if self.use_consul and consulate is not None:
            consul = consulate.Consul(host=self.consul_ip_addr)
            params['sentinels'] = [
                (node['Address'], self.sentinel_port) for node in consul.catalog.nodes() 
                if node['Meta'].get('consul_role') == 'server'
            ]
        else:
            params['sentinels'] = self.sentinels

        return get_redis_via_sentinel(
            redis_class=type(str('Redis'), (EnsuredRedisMixin, Redis), {}),
            **params
        )
