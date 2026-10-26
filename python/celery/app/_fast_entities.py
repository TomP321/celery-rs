"""Exchange/Queue leggeri per il percorso producer/consumer Rust.

Il riferimento crea ``kombu.Exchange``/``kombu.Queue`` anche quando l'I/O passa
dal core Rust: importare ``kombu.entity`` trascina ``kombu.serialization``
(yaml/msgpack), ``kombu.connection`` (py-amqp), ``billiard``/cffi e
``importlib.metadata`` nel processo client (spec 08b). Queste classi hanno gli
stessi campi e la stessa semantica di dichiarazione usata da celery-rs; i
metodi che parlano davvero con un canale kombu (``declare``/``bind``) passano
alla classe originale, importata solo in quel momento.
"""
from __future__ import annotations

import numbers

__all__ = ('DELIVERY_MODES', 'Exchange', 'Queue', 'maybe_delivery_mode')

TRANSIENT_DELIVERY_MODE = 1
PERSISTENT_DELIVERY_MODE = 2
DELIVERY_MODES = {'transient': TRANSIENT_DELIVERY_MODE,
                  'persistent': PERSISTENT_DELIVERY_MODE}

INTERNAL_EXCHANGE_PREFIX = ('amq.',)


def maybe_delivery_mode(v, modes=None, default=PERSISTENT_DELIVERY_MODE):
    """Get delivery mode by name (or none if undefined)."""
    modes = DELIVERY_MODES if not modes else modes
    if v:
        return v if isinstance(v, numbers.Integral) else modes[v]
    return default


def _kombu_exchange():
    from kombu import Exchange as KombuExchange
    return KombuExchange


def _kombu_queue():
    from kombu import Queue as KombuQueue
    return KombuQueue


class _FastEntity:
    """Campi comuni: dichiarazione, hash e conversione a kombu."""

    attrs = ()  # type: tuple

    @property
    def is_bound(self):
        return False

    def as_dict(self, recurse=False):
        """Come ``kombu.abstract.Object.as_dict`` (usato da inspect)."""
        def f(obj, type_=None):
            if recurse and isinstance(obj, _FastEntity):
                return obj.as_dict(recurse=True)
            return type_(obj) if type_ and obj is not None else obj
        return {attr: f(getattr(self, attr, None), type_)
                for attr, type_ in self.attrs}

    def __hash__(self):
        return hash(f'{self._HASH_PREFIX}|{self.name}')

    def _as_kombu(self):
        raise NotImplementedError

    def bind(self, channel):
        return self._as_kombu()(channel)

    def unbind(self, *args, **kwargs):
        return self._as_kombu().unbind(*args, **kwargs)

    def declare(self, *args, **kwargs):
        return self._as_kombu().declare(*args, **kwargs)

    def delete(self, *args, **kwargs):
        return self._as_kombu().delete(*args, **kwargs)

    def queue_declare(self, *args, **kwargs):
        from kombu.entity import Queue as KombuQueue
        return KombuQueue.queue_declare(self, *args, **kwargs)

    def __eq__(self, other):
        if isinstance(other, (_FastEntity,)):
            return self._cmp_fields() == other._cmp_fields()
        return NotImplemented

    def __ne__(self, other):
        return not self.__eq__(other)

    def _cmp_fields(self):
        return tuple(getattr(self, name, None) for name, _ in self.attrs)

    def __repr__(self):
        return self._repr_entity(f'{type(self).__name__} {self.name!r}')


class Exchange(_FastEntity):
    """Sostituto leggero di :class:`kombu.Exchange`."""

    _HASH_PREFIX = 'E'
    name = ''
    type = 'direct'
    durable = True
    auto_delete = False
    passive = False
    delivery_mode = None
    no_declare = False
    arguments = None

    attrs = (
        ('name', None),
        ('type', None),
        ('arguments', None),
        ('durable', bool),
        ('passive', bool),
        ('auto_delete', bool),
        ('delivery_mode', lambda m: DELIVERY_MODES.get(m) or m),
        ('no_declare', bool),
    )

    def __init__(self, name='', type='', channel=None, routing_key=None,
                 **kwargs):
        self.name = name or self.name
        self.type = type or self.type
        for attr in ('durable', 'auto_delete', 'passive', 'delivery_mode',
                     'no_declare', 'arguments'):
            if attr in kwargs and kwargs[attr] is not None:
                setattr(self, attr, kwargs[attr])
        self.channel = channel

    def __call__(self, channel=None):
        # MaybeChannelBound: un Exchange "legato" è quello kombu.
        return self._as_kombu()(channel or self.channel)

    def _as_kombu(self):
        return _kombu_exchange()(
            self.name, self.type, durable=self.durable,
            auto_delete=self.auto_delete, delivery_mode=self.delivery_mode,
            arguments=self.arguments, no_declare=self.no_declare,
            passive=self.passive,
        )

    @property
    def can_cache_declaration(self):
        return not self.auto_delete

    def __repr__(self):
        return (f'<Exchange {self.name!r}({self.type!r})'
                f'{" bound to " + repr(self.channel) if self.channel else ""}>')


class Queue(_FastEntity):
    """Sostituto leggero di :class:`kombu.Queue`."""

    _HASH_PREFIX = 'Q'
    durable = True
    exclusive = False
    auto_delete = False
    no_ack = False
    no_declare = False
    expires = None
    message_ttl = None
    max_length = None
    max_length_bytes = None
    max_priority = None
    queue_arguments = None
    binding_arguments = None
    consumer_arguments = None
    alias = None
    on_declared = None

    attrs = (
        ('name', None),
        ('exchange', None),
        ('routing_key', None),
        ('queue_arguments', None),
        ('binding_arguments', None),
        ('consumer_arguments', None),
        ('durable', bool),
        ('exclusive', bool),
        ('auto_delete', bool),
        ('no_ack', None),
        ('alias', None),
        ('bindings', list),
        ('no_declare', bool),
        ('expires', float),
        ('message_ttl', float),
        ('max_length', int),
        ('max_length_bytes', int),
        ('max_priority', int),
    )

    def __init__(self, name='', exchange=None, routing_key='',
                 channel=None, bindings=None, on_declared=None,
                 routing_key_override=None, **kwargs):
        self.name = name or ''
        self.routing_key = routing_key or ''
        self.bindings = set(bindings or [])
        self.on_declared = on_declared
        self.channel = channel
        if isinstance(exchange, str):
            self.exchange = Exchange(exchange)
        elif exchange is not None:
            self.exchange = exchange
        else:
            self.exchange = None
        if isinstance(exchange, (list, tuple, set)):
            self.bindings |= set(exchange)
            self.exchange = None
        for attr in ('durable', 'exclusive', 'auto_delete', 'no_ack',
                     'alias', 'no_declare', 'queue_arguments',
                     'binding_arguments', 'consumer_arguments', 'expires',
                     'message_ttl', 'max_length', 'max_length_bytes',
                     'max_priority'):
            if attr in kwargs and kwargs[attr] is not None:
                setattr(self, attr, kwargs[attr])
        if self.exclusive:
            self.auto_delete = True

    def __call__(self, channel=None):
        return self._as_kombu()(channel or self.channel)

    def _as_kombu(self):
        exchange = self.exchange
        if isinstance(exchange, Exchange):
            exchange = exchange._as_kombu()
        return _kombu_queue()(
            self.name, exchange, self.routing_key,
            durable=self.durable, exclusive=self.exclusive,
            auto_delete=self.auto_delete, no_ack=self.no_ack,
            queue_arguments=self.queue_arguments,
            binding_arguments=self.binding_arguments,
            consumer_arguments=self.consumer_arguments,
            alias=self.alias, bindings=self.bindings,
            on_declared=self.on_declared, no_declare=self.no_declare,
            expires=self.expires, message_ttl=self.message_ttl,
            max_length=self.max_length,
            max_length_bytes=self.max_length_bytes,
            max_priority=self.max_priority,
        )

    @property
    def can_cache_declaration(self):
        if self.queue_arguments:
            expiring_queue = 'x-expires' in self.queue_arguments
        else:
            expiring_queue = False
        return not expiring_queue and not self.auto_delete

    def _cmp_fields(self):
        return (
            self.name, self.exchange, self.routing_key, self.queue_arguments,
            self.binding_arguments, self.consumer_arguments, self.durable,
            self.exclusive, self.auto_delete,
        )

    @classmethod
    def from_dict(cls, queue, **options):
        binding_key = options.get('binding_key') or options.get('routing_key')
        e_durable = options.get('exchange_durable')
        if e_durable is None:
            e_durable = options.get('durable')
        e_auto_delete = options.get('exchange_auto_delete')
        if e_auto_delete is None:
            e_auto_delete = options.get('auto_delete')
        q_durable = options.get('queue_durable')
        if q_durable is None:
            q_durable = options.get('durable')
        q_auto_delete = options.get('queue_auto_delete')
        if q_auto_delete is None:
            q_auto_delete = options.get('auto_delete')
        exchange = Exchange(
            options.get('exchange'),
            type=options.get('exchange_type'),
            delivery_mode=options.get('delivery_mode'),
            routing_key=options.get('routing_key'),
            durable=e_durable,
            auto_delete=e_auto_delete,
            arguments=options.get('exchange_arguments'),
        )
        return cls(
            queue, exchange, binding_key,
            durable=q_durable,
            auto_delete=q_auto_delete,
            queue_arguments=options.get('queue_arguments'),
            binding_arguments=options.get('binding_arguments'),
            consumer_arguments=options.get('consumer_arguments'),
            expires=options.get('expires'),
            message_ttl=options.get('message_ttl'),
            max_length=options.get('max_length'),
            max_length_bytes=options.get('max_length_bytes'),
            max_priority=options.get('max_priority'),
            alias=options.get('alias'),
            no_declare=options.get('no_declare'),
        )

    def __repr__(self):
        if self.bindings:
            return f'<Queue {self.name!r} -> {sorted(self.bindings, key=str)}>'
        return (f'<Queue {self.name!r} -> {self.exchange!r}'
                f' -> {self.routing_key!r}>')
