"""Celery Command Line Interface."""
import importlib
import os
import pathlib
import sys
import traceback
from importlib.metadata import entry_points

import click
import click.exceptions
from click_didyoumean import DYMGroup
from click_plugins import with_plugins

from celery import VERSION_BANNER
from celery.app.utils import find_app
from celery.bin.base import CeleryCommand, CeleryOption, CLIContext, consume_preload_options

#: Comandi caricati solo quando servono davvero (help, esecuzione o `compat
#: api`). Le import qui sotto sarebbero tutte pagate da ogni invocazione,
#: anche da `celery call`/`status`.
_COMMANDS = {
    'amqp': ('celery.bin.amqp', 'amqp'),
    'beat': ('celery.bin.beat', 'beat'),
    'call': ('celery.bin.call', 'call'),
    'control': ('celery.bin.control', 'control'),
    'events': ('celery.bin.events', 'events'),
    'graph': ('celery.bin.graph', 'graph'),
    'inspect': ('celery.bin.control', 'inspect'),
    'list': ('celery.bin.list', 'list_'),
    'logtool': ('celery.bin.logtool', 'logtool'),
    'migrate': ('celery.bin.migrate', 'migrate'),
    'multi': ('celery.bin.multi', 'multi'),
    'purge': ('celery.bin.purge', 'purge'),
    'result': ('celery.bin.result', 'result'),
    'shell': ('celery.bin.shell', 'shell'),
    'status': ('celery.bin.control', 'status'),
    'upgrade': ('celery.bin.upgrade', 'upgrade'),
    'worker': ('celery.bin.worker', 'worker'),
}

#: Nomi pubblici del modulo (come nel riferimento, ``list_`` incluso) → comando.
#: Risolti al primo accesso da ``__getattr__`` (PEP 562): ``from
#: celery.bin.celery import worker`` restituisce il comando vero.
_ATTRIBUTES = {('list_' if name == 'list' else name): entry
               for name, entry in _COMMANDS.items()}


def __getattr__(name):
    entry = _ATTRIBUTES.get(name)
    if entry is None:
        raise AttributeError(f'module {__name__!r} has no attribute {name!r}')
    module, attribute = entry
    value = getattr(importlib.import_module(module), attribute)
    globals()[name] = value
    return value


def __dir__():
    return sorted(set(globals()) | set(_ATTRIBUTES))


class _LazyCeleryGroup(DYMGroup):
    """``DYMGroup`` che importa i sottocomandi su richiesta.

    ``self.commands`` contiene solo i comandi già materializzati (``report`` e
    i plugin); ``get_command`` importa il modulo del comando richiesto.
    L'accesso diretto a ``commands`` (``compat api``, plugin, terze parti)
    materializza tutto, così la superficie pubblica resta identica.
    """

    def __init__(self, *args, **kwargs):
        self._commands = {}
        self._lazy = dict(_COMMANDS)
        super().__init__(*args, **kwargs)

    def _load_all(self):
        for name in list(self._lazy):
            self.get_command(None, name)

    @property
    def commands(self):
        self._load_all()
        return self._commands

    @commands.setter
    def commands(self, value):
        self._commands = value

    def add_command(self, cmd, name=None):
        name = name or cmd.name
        if name is None:
            raise TypeError("Command has no name.")
        self._commands[name] = cmd

    def get_command(self, ctx, cmd_name):
        command = self._commands.get(cmd_name)
        if command is None and cmd_name in self._lazy:
            module, attribute = self._lazy[cmd_name]
            command = getattr(importlib.import_module(module), attribute)
            self._lazy.pop(cmd_name, None)
            self._commands[cmd_name] = command
        return command

    def list_commands(self, ctx):
        return sorted(set(self._commands) | set(self._lazy))


UNABLE_TO_LOAD_APP_MODULE_NOT_FOUND = click.style("""
Unable to load celery application.
The module {0} was not found.""", fg='red')

UNABLE_TO_LOAD_APP_ERROR_OCCURRED = click.style("""
Unable to load celery application.
While trying to load the module {0} the following error occurred:
{1}""", fg='red')

UNABLE_TO_LOAD_APP_APP_MISSING = click.style("""
Unable to load celery application.
{0}""")


if sys.version_info >= (3, 10):
    _PLUGINS = entry_points(group='celery.commands')
else:
    try:
        _PLUGINS = entry_points().get('celery.commands', [])
    except AttributeError:
        _PLUGINS = entry_points().select(group='celery.commands')


@with_plugins(_PLUGINS)
@click.group(cls=_LazyCeleryGroup, invoke_without_command=True)
@click.option('-A',
              '--app',
              envvar='APP',
              cls=CeleryOption,
              # May take either: a str when invoked from command line (Click),
              # or a Celery object when invoked from inside Celery; hence the
              # need to prevent Click from "processing" the Celery object and
              # converting it into its str representation.
              type=click.UNPROCESSED,
              help_group="Global Options")
@click.option('-b',
              '--broker',
              envvar='BROKER_URL',
              cls=CeleryOption,
              help_group="Global Options")
@click.option('--result-backend',
              envvar='RESULT_BACKEND',
              cls=CeleryOption,
              help_group="Global Options")
@click.option('--loader',
              envvar='LOADER',
              cls=CeleryOption,
              help_group="Global Options")
@click.option('--config',
              envvar='CONFIG_MODULE',
              cls=CeleryOption,
              help_group="Global Options")
@click.option('--workdir',
              cls=CeleryOption,
              type=pathlib.Path,
              callback=lambda _, __, wd: os.chdir(wd) if wd else None,
              is_eager=True,
              help_group="Global Options")
@click.option('-C',
              '--no-color',
              envvar='NO_COLOR',
              is_flag=True,
              cls=CeleryOption,
              help_group="Global Options")
@click.option('-q',
              '--quiet',
              is_flag=True,
              cls=CeleryOption,
              help_group="Global Options")
@click.option('--version',
              cls=CeleryOption,
              is_flag=True,
              help_group="Global Options")
@click.option('--skip-checks',
              envvar='SKIP_CHECKS',
              cls=CeleryOption,
              is_flag=True,
              help_group="Global Options",
              help="Skip Django core checks on startup. Setting the SKIP_CHECKS environment "
                   "variable to any non-empty string will have the same effect.")
@click.pass_context
def celery(ctx, app, broker, result_backend, loader, config, workdir,
           no_color, quiet, version, skip_checks):
    """Celery command entrypoint."""
    if version:
        click.echo(VERSION_BANNER)
        ctx.exit()
    elif ctx.invoked_subcommand is None:
        click.echo(ctx.get_help())
        ctx.exit()

    if loader:
        # Default app takes loader from this env (Issue #1066).
        os.environ['CELERY_LOADER'] = loader
    if broker:
        os.environ['CELERY_BROKER_URL'] = broker
    if result_backend:
        os.environ['CELERY_RESULT_BACKEND'] = result_backend
    if config:
        os.environ['CELERY_CONFIG_MODULE'] = config
    if skip_checks:
        os.environ['CELERY_SKIP_CHECKS'] = 'true'

    if isinstance(app, str):
        try:
            app = find_app(app)
        except ModuleNotFoundError as e:
            if e.name != app:
                exc = traceback.format_exc()
                ctx.fail(
                    UNABLE_TO_LOAD_APP_ERROR_OCCURRED.format(app, exc)
                )
            ctx.fail(UNABLE_TO_LOAD_APP_MODULE_NOT_FOUND.format(e.name))
        except AttributeError as e:
            attribute_name = e.args[0].capitalize()
            ctx.fail(UNABLE_TO_LOAD_APP_APP_MISSING.format(attribute_name))
        except Exception:
            exc = traceback.format_exc()
            ctx.fail(
                UNABLE_TO_LOAD_APP_ERROR_OCCURRED.format(app, exc)
            )

    ctx.obj = CLIContext(app=app, no_color=no_color, workdir=workdir,
                         quiet=quiet)

    # User options
    for name in ('worker', 'beat', 'events'):
        options = ctx.obj.app.user_options.get(name, ())
        if options:
            celery.get_command(ctx, name).params.extend(options)

    # Plugin callbacks such as Flower do not declare these options. Take
    # them off the keyword arguments before Click calls the callback (#7894).
    preload_options = ctx.obj.app.user_options.get('preload', ())
    if preload_options:
        for command in celery.commands.values():
            command.params.extend(preload_options)
            command.callback = consume_preload_options(command.callback)


@celery.command(cls=CeleryCommand)
@click.pass_context
def report(ctx, **kwargs):
    """Shows information useful to include in bug-reports."""
    app = ctx.obj.app
    app.loader.import_default_modules()
    ctx.obj.echo(app.bugreport())


# Monkey-patch click to display a custom error
# when -A or --app are used as sub-command options instead of as options
# of the global command.

previous_show_implementation = click.exceptions.NoSuchOption.show

WRONG_APP_OPTION_USAGE_MESSAGE = """You are using `{option_name}` as an option of the {info_name} sub-command:
celery {info_name} {option_name} celeryapp <...>

The support for this usage was removed in Celery 5.0. Instead you should use `{option_name}` as a global option:
celery {option_name} celeryapp {info_name} <...>"""


def _show(self, file=None):
    if self.option_name in ('-A', '--app'):
        self.ctx.obj.error(
            WRONG_APP_OPTION_USAGE_MESSAGE.format(
                option_name=self.option_name,
                info_name=self.ctx.info_name),
            fg='red'
        )
    previous_show_implementation(self, file=file)


click.exceptions.NoSuchOption.show = _show


def main() -> int:
    """Start celery umbrella command.

    This function is the main entrypoint for the CLI.

    :return: The exit code of the CLI.
    """
    return celery(auto_envvar_prefix="CELERY")
