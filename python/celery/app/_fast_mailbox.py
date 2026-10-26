"""Mailbox leggero per ``Control``: nessun import di ``kombu.pidbox``.

Il worker master crea ``app.control`` già all'avvio (mingle). Il mailbox di
kombu importa ``kombu.pidbox`` e, con esso, ``kombu.connection``: qui la parte
che serve al client di controllo Rust (exchange, code, clock, ``unclaimed``) è
costruita con i tipi di ``kombu.entity``, mentre qualunque operazione che
richiede davvero il mailbox di kombu (``_broadcast`` con una connessione
esplicita, ``Node``, ...) lo importa al primo uso.
"""
from __future__ import annotations

from collections import defaultdict, deque
from copy import copy
from time import time

from kombu import Exchange, Queue
from kombu.clocks import LamportClock
from kombu.utils.uuid import uuid

__all__ = ('FastMailbox',)

REPLY_QUEUE_EXPIRES = 10


class FastMailbox:
    """Sottoinsieme di ``kombu.pidbox.Mailbox`` usato dal worker e dal client.

    I parametri sono quelli del mailbox del riferimento; il comportamento
    completo resta disponibile tramite :meth:`as_real`.
    """

    node_cls = None
    exchange_fmt = '%s.pidbox'
    reply_exchange_fmt = 'reply.%s.pidbox'

    def __init__(self, namespace,
                 type='direct', connection=None, clock=None,
                 accept=None, serializer=None, producer_pool=None,
                 queue_ttl=None, queue_expires=None,
                 queue_durable=False, queue_exclusive=True,
                 reply_queue_ttl=None, reply_queue_expires=10.0):
        if queue_exclusive and queue_durable:
            raise ValueError(
                "queue_exclusive and queue_durable cannot both be True "
                "(exclusive queues are automatically deleted and cannot be durable).",
            )
        self.namespace = namespace
        self.connection = connection
        self.type = type
        self.clock = LamportClock() if clock is None else clock
        self.exchange = self._get_exchange(self.namespace, self.type)
        self.reply_exchange = self._get_reply_exchange(self.namespace)
        self.unclaimed = defaultdict(deque)
        self.accept = ['json'] if accept is None else accept
        self.serializer = serializer
        self.queue_ttl = queue_ttl
        self.queue_expires = queue_expires
        self.queue_durable = queue_durable
        self.queue_exclusive = queue_exclusive
        self.reply_queue_ttl = reply_queue_ttl
        self.reply_queue_expires = reply_queue_expires
        self._producer_pool = producer_pool

    # --------------------------------------------------------------- costruzione

    def _get_exchange(self, namespace, type):
        return Exchange(self.exchange_fmt % namespace,
                        type=type,
                        durable=False,
                        delivery_mode='transient')

    def _get_reply_exchange(self, namespace):
        return Exchange(self.reply_exchange_fmt % namespace,
                        type='direct',
                        durable=False,
                        delivery_mode='transient')

    def get_queue(self, hostname):
        return Queue(
            f'{hostname}.{self.namespace}.pidbox',
            exchange=self.exchange,
            durable=self.queue_durable,
            exclusive=self.queue_exclusive,
            auto_delete=not self.queue_durable,
            expires=self.queue_expires,
            message_ttl=self.queue_ttl,
        )

    def get_reply_queue(self):
        oid = self.oid
        return Queue(
            f'{oid}.{self.reply_exchange.name}',
            exchange=self.reply_exchange,
            routing_key=oid,
            durable=self.queue_durable,
            exclusive=self.queue_exclusive,
            auto_delete=not self.queue_durable,
            expires=self.reply_queue_expires,
            message_ttl=self.reply_queue_ttl,
        )

    @property
    def reply_queue(self):
        return self.get_reply_queue()

    @property
    def oid(self):
        from kombu.common import oid_from
        return oid_from(self)

    @property
    def producer_pool(self):
        from kombu.utils.functional import maybe_evaluate
        return maybe_evaluate(self._producer_pool)

    @producer_pool.deleter
    def producer_pool(self):
        # Come `cached_property` nel mailbox di kombu: dopo il fork il pool va
        # ricreato. Qui non c'è cache (la `lazy` valuta `app.amqp` a ogni
        # accesso), quindi non c'è nulla da invalidare.
        pass

    def __call__(self, connection):
        """Mailbox di kombu legato a *connection* (solo sul percorso classico)."""
        bound = self.as_real()
        return bound(connection)

    def as_real(self):
        """Un vero ``kombu.pidbox.Mailbox`` con gli stessi parametri."""
        from kombu.pidbox import Mailbox
        return Mailbox(
            self.namespace,
            type=self.type,
            connection=self.connection,
            clock=self.clock,
            accept=self.accept,
            serializer=self.serializer,
            producer_pool=self._producer_pool,
            queue_ttl=self.queue_ttl,
            queue_expires=self.queue_expires,
            queue_durable=self.queue_durable,
            queue_exclusive=self.queue_exclusive,
            reply_queue_ttl=self.reply_queue_ttl,
            reply_queue_expires=self.reply_queue_expires,
        )

    def Node(self, hostname=None, state=None, channel=None, handlers=None):
        return self.as_real().Node(
            hostname, state=state, channel=channel, handlers=handlers)

    # --------------------------------------------------------------- API kombu

    def _broadcast(self, *args, **kwargs):
        return self.as_real()._broadcast(*args, **kwargs)

    def _publish(self, *args, **kwargs):
        return self.as_real()._publish(*args, **kwargs)

    def _collect(self, *args, **kwargs):
        return self.as_real()._collect(*args, **kwargs)

    def _publish_reply(self, *args, **kwargs):
        return self.as_real()._publish_reply(*args, **kwargs)

    def call(self, *args, **kwargs):
        return self.as_real().call(*args, **kwargs)

    def cast(self, *args, **kwargs):
        return self.as_real().cast(*args, **kwargs)

    def abcast(self, *args, **kwargs):
        return self.as_real().abcast(*args, **kwargs)

    def multi_call(self, *args, **kwargs):
        return self.as_real().multi_call(*args, **kwargs)

    def __copy__(self):
        bound = copy(self)
        bound.unclaimed = defaultdict(deque)
        return bound


# Compatibilità della superficie pubblica: `Control.Mailbox` è il mailbox di
# kombu agli occhi di chi ispeziona la classe (stesso origin, stessa forma).
FastMailbox.__module__ = 'kombu.pidbox'
FastMailbox.__qualname__ = 'Mailbox'
