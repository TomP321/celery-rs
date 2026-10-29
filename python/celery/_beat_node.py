"""Control node loaded when beat enables pidbox."""
from kombu import pidbox


class _BeatNode(pidbox.Node):
    """Pidbox node that stays silent while the scheduler is stalled.

    The staleness check lives here rather than in the consumer callback
    because :meth:`kombu.pidbox.Node.handle_message` only reaches
    :meth:`dispatch` for messages actually addressed to this node.
    Checking earlier would make beat log about every broadcast aimed at
    a worker.
    """

    #: owning :class:`BeatPidbox`, set once it has built this node.
    beat_pidbox = None

    def dispatch(self, method, arguments=None, reply_to=None,
                 ticket=None, **kwargs):
        owner = self.beat_pidbox
        if owner is not None and owner.is_stale():
            from .beat import warning
            warning('beat pidbox: not answering %s, the scheduler has '
                    'not ticked for %.1fs (over the %ss limit).',
                    method, owner.tick_age(), owner.max_tick_age)
            return None
        return super().dispatch(method, arguments, reply_to, ticket,
                                **kwargs)

