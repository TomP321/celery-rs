"""Test differenziali contro Celery 5.7.0rc1 (.venv-ref).

Tutti i confronti avvengono nello stesso processo: riferimento e ``_celery_rs``.
"""

from __future__ import annotations

import datetime
import decimal
import enum
import json
import math
import random
import threading
import time
import uuid
from collections import namedtuple
from fractions import Fraction

import pytest

import _celery_rs as rs
import kombu.compression as kcomb
import kombu.serialization as kser
import kombu.utils.json as kjson
from celery import Celery
from celery.canvas import Signature
from celery.utils.saferepr import saferepr as ref_saferepr
from celery.utils.text import truncate as ref_truncate

SEED = 20261023

NT = namedtuple("NT", "a b")


class Color(enum.IntEnum):
    RED = 1
    GREEN = 2


class JsonObject:
    def __init__(self, value):
        self.value = value

    def __json__(self):
        return {"json": self.value}


class ListSubclass(list):
    pass


class DictSubclass(dict):
    pass


class TupleSubclass(tuple):
    pass


class SetSubclass(set):
    pass


class FrozenSetSubclass(frozenset):
    pass


class NumberLike:
    pass


class StrSubclass(str):
    pass


class IntSubclass(int):
    def __repr__(self):
        return "INT_SUBCLASS_REPR"


class FloatSubclass(float):
    def __repr__(self):
        return "FLOAT_SUBCLASS_REPR"


class StrEnum(str, enum.Enum):
    A = "a"
    B = "b"


class CustomRepr:
    def __init__(self, text):
        self.text = text

    def __repr__(self):
        return self.text


class HashableDict(dict):
    def __hash__(self):
        return id(self)


DEFAULT = kjson.JSONEncoder().default


def same(a, b) -> bool:
    if isinstance(a, float) and isinstance(b, float) and math.isnan(a) and math.isnan(b):
        return True
    if (
        isinstance(a, decimal.Decimal)
        and isinstance(b, decimal.Decimal)
        and a.is_nan()
        and b.is_nan()
    ):
        return True
    if type(a) is not type(b):
        return False
    if isinstance(a, list):
        return len(a) == len(b) and all(same(x, y) for x, y in zip(a, b))
    if isinstance(a, tuple):
        return len(a) == len(b) and all(same(x, y) for x, y in zip(a, b))
    if isinstance(a, dict):
        return list(a.keys()) == list(b.keys()) and all(
            same(a[k], b[k]) for k in a
        )
    return a == b


def random_string(rng):
    if rng.random() < 0.06:
        # Stringa lunga con spazi: esercita truncate con maxlen 64/1024.
        target = rng.randrange(220, 420)
        parts = []
        total = 0
        while total < target:
            word = "".join(
                chr(rng.choice([rng.randrange(0x20, 0x7F), rng.randrange(0xA0, 0x2FFF)]))
                for _ in range(rng.randrange(3, 10))
            )
            parts.append(word)
            total += len(word) + 1
        return " ".join(parts)
    n = rng.randrange(0, 8)
    chars = []
    for _ in range(n):
        kind = rng.randrange(6)
        if kind == 0:
            chars.append(chr(rng.randrange(0x20, 0x7F)))
        elif kind == 1:
            chars.append(chr(rng.randrange(0xA0, 0x2FFF)))
        elif kind == 2:
            chars.append(chr(rng.randrange(0x10000, 0x1F600)))
        elif kind == 3:
            chars.append(chr(rng.randrange(0, 0x20)))
        elif kind == 4:
            chars.append(rng.choice(["'", '"', "\\", "\n", "\r", "\t"]))
        else:
            # Surrogato isolato.
            chars.append(chr(rng.randrange(0xD800, 0xE000)))
    return "".join(chars)


FLOATS = [
    0.1,
    1e16,
    1e-5,
    -0.0,
    5e-324,
    1.7976931348623157e308,
    float("nan"),
    float("inf"),
    float("-inf"),
    0.0,
    1.0,
    -1.0,
    1e15,
    1e-4,
    123456789.123456789,
]


def random_value(rng, depth=0):
    if depth <= 0:
        return rng.choice(PRIMITIVES)(rng)
    kind = rng.randrange(14)
    if kind < 4:
        return [random_value(rng, depth - 1) for _ in range(rng.randrange(4))]
    if kind == 4:
        return tuple(random_value(rng, depth - 1) for _ in range(rng.randrange(4)))
    if kind < 8:
        keys = [
            "a",
            "b",
            1,
            1.5,
            True,
            None,
            "é",
            Color.RED,
            StrEnum.A,
            IntSubclass(3),
            FloatSubclass(1.5),
        ]
        return {
            rng.choice(keys): random_value(rng, depth - 1)
            for _ in range(rng.randrange(4))
        }
    return rng.choice(PRIMITIVES)(rng)


PRIMITIVES = [
    lambda rng: random_string(rng),
    lambda rng: rng.choice(
        [0, 1, -1, 2**63, -(2**63), 2**80, -(2**80), rng.randrange(-10**9, 10**9)]
    ),
    lambda rng: rng.choice(FLOATS),
    lambda rng: rng.random() * rng.choice([1e-20, 1e20, 1.0]),
    lambda rng: rng.choice([True, False]),
    lambda rng: None,
    lambda rng: NT(rng.randrange(5), random_string(rng)),
    lambda rng: Color(rng.choice([1, 2])),
    lambda rng: datetime.datetime(2020, 1, 2, 3, 4, 5),
    lambda rng: datetime.datetime(
        2020, 1, 2, 3, 4, 5, tzinfo=datetime.timezone.utc
    ),
    lambda rng: datetime.date(2020, 1, 2),
    lambda rng: datetime.time(1, 2, 3, 456),
    lambda rng: decimal.Decimal(rng.choice(["1.230", "-0", "NaN", "1E+10"])),
    lambda rng: uuid.UUID(int=rng.getrandbits(128)),
    lambda rng: bytes(rng.randrange(7)),
    lambda rng: b"\xff\x00\xfe",
    lambda rng: JsonObject(random_value(rng)),
    lambda rng: Signature("x", args=(1, rng.randrange(5))),
    lambda rng: StrSubclass(random_string(rng)),
    lambda rng: IntSubclass(rng.randrange(-10**6, 10**6)),
    lambda rng: FloatSubclass(rng.choice([0.1, 1.5, -0.0, 1e16])),
    lambda rng: rng.choice([StrEnum.A, StrEnum.B]),
]


def many_values(n, depth=6):
    rng = random.Random(SEED)
    for _ in range(n):
        yield random_value(rng, rng.randrange(depth + 1))


# ---------------------------------------------------------------------------
# json_dumps
# ---------------------------------------------------------------------------


def test_json_dumps_50k():
    rng = random.Random(SEED)
    for i, value in enumerate(many_values(50_000)):
        expected = kjson.dumps(value)
        got = rs.json_dumps(value, DEFAULT).decode("utf-8")
        assert got == expected, f"valore #{i}: {value!r}"


def test_json_dumps_errori():
    cycle = []
    cycle.append(cycle)
    cases = [
        ({("a", 1): "x"}, TypeError),
        ({decimal.Decimal("1.5"): 1}, TypeError),
        (object(), TypeError),
        (cycle, ValueError),
    ]
    for value, exc_type in cases:
        with pytest.raises(exc_type) as ref_err:
            kjson.dumps(value)
        with pytest.raises(exc_type) as got_err:
            rs.json_dumps(value, DEFAULT)
        assert str(got_err.value) == str(ref_err.value)


def test_json_dumps_sottoclassi_e_chiavi():
    values = [
        Signature("x", args=(1,)),
        NT(1, 2),
        Color.RED,
        ListSubclass([1, 2]),
        DictSubclass(a=1),
        TupleSubclass([1, 2]),
        {1: "a", 1.5: "b", True: "c", None: "d"},
    ]
    for value in values:
        assert rs.json_dumps(value, DEFAULT).decode() == kjson.dumps(value)


# ---------------------------------------------------------------------------
# json_loads
# ---------------------------------------------------------------------------


CORPUS = [
    "",
    " ",
    "\t\n",
    "[",
    "[1",
    "[1,",
    "[1 2]",
    "[1,]",
    "{",
    '{"a"',
    '{"a":',
    '{"a":1',
    '{"a":1,',
    '{"a":1,}',
    "{1:2}",
    "{,}",
    '{"a" 1}',
    "tru",
    "nul",
    "NaN",
    "-Infinity",
    "Infinity",
    "+1",
    ".5",
    "1.",
    "1e",
    "1e+",
    "01",
    "-",
    "-a",
    "1 2",
    '"abc',
    '"abc\\',
    '"\\x"',
    '"\\u12"',
    '"\\u12g4"',
    '"a\x01b"',
    '{"a":1}extra',
    '{"a":1}\n x',
    "3.14x",
    "[1,] ",
    "\ufeff{}",
    "null",
    "true",
    "false",
    "[]",
    "{}",
    "[[]]",
    '{"a":{}}',
    '{"a":[1,2,3],"b":null}',
    '"é"',
    '"\\u00e9"',
    '"\\ud83d\\ude00"',
    '"\\ud800"',
    "123456789012345678901234567890",
    "-0",
    "-0.0",
    "1e400",
    "-1e400",
    "0.1e2",
    "[1e999]",
    '{"__type__":"bytes","__value__":"abc"}',
    '{"__type__":"base64","__value__":"/w=="}',
    '{"__type__":"nope","__value__":1}',
    '{"__type__":1,"__value__":2}',
    '{"__type__":1}',
    '{"a":1,"__type__":2,"__value__":3}',
    '"\\uD800\\uDC00"',
    '"\\uD800\\u0041"',
    '{"a":1,"b":2}',
    '{"a":1,"a":2}',
    "[1,[2,[3]]]",
    '"\\uD800x"',
    '"\\uDC00"',
    '{"a":\n1\t, "b"\r: 2}',
]


def outcome(func, data):
    try:
        return ("ok", func(data))
    except Exception as exc:  # noqa: BLE001 - confrontiamo il tipo esatto
        return ("err", type(exc), getattr(exc, "pos", None), str(exc))


def test_json_loads_corpus():
    for text in CORPUS:
        ref = outcome(kjson.loads, text)
        got = outcome(lambda data: rs.json_loads(data, kjson.object_hook), text)
        assert got[0] == ref[0], (text, ref, got)
        if ref[0] == "ok":
            assert same(got[1], ref[1]), text
        else:
            # Tipo, posizione e messaggio di JSONDecodeError.
            assert got[1:] == ref[1:], text


def test_json_loads_roundtrip_50k():
    for i, value in enumerate(many_values(50_000)):
        encoded = kjson.dumps(value)
        ref = kjson.loads(encoded)
        got = rs.json_loads(encoded, kjson.object_hook)
        assert same(got, ref), f"valore #{i}: {value!r}"


def test_json_loads_tipi_input():
    body = b'{"a": [1, 2]}'
    expected = kjson.loads(body)
    assert rs.json_loads(body, kjson.object_hook) == expected
    assert rs.json_loads(bytearray(body), kjson.object_hook) == expected
    assert rs.json_loads(memoryview(body), kjson.object_hook) == expected
    assert rs.json_loads(body.decode(), kjson.object_hook) == expected
    with pytest.raises(TypeError):
        rs.json_loads(1, kjson.object_hook)


# ---------------------------------------------------------------------------
# saferepr
# ---------------------------------------------------------------------------


REPR_OBJECTS = [
    lambda rng: random_string(rng),
    lambda rng: rng.choice([1, -1, 2**80, True, False, None, 1.5, 1 + 2j]),
    lambda rng: decimal.Decimal(rng.choice(["1.20", "-0", "NaN"])),
    lambda rng: rng.choice([b"abc", b"\xff", b"a'b", bytes(range(8))]),
    lambda rng: set(rng.sample(range(10), rng.randrange(4))),
    lambda rng: frozenset(rng.sample(range(10), rng.randrange(4))),
    lambda rng: ListSubclass(rng.sample(range(10), rng.randrange(4))),
    lambda rng: DictSubclass(a=rng.randrange(5)),
    lambda rng: TupleSubclass(rng.sample(range(10), rng.randrange(4))),
    lambda rng: SetSubclass(rng.sample(range(10), rng.randrange(4))),
    lambda rng: FrozenSetSubclass(rng.sample(range(10), rng.randrange(4))),
    lambda rng: range(rng.randrange(5)),
    lambda rng: Fraction(rng.randrange(-5, 5), rng.randrange(1, 5)),
    lambda rng: rng.choice([0.5, -1.25, 1e16]),
    lambda rng: complex(rng.randrange(-3, 4), rng.randrange(-3, 4)),
    lambda rng: StrSubclass(random_string(rng)),
    lambda rng: CustomRepr(rng.choice(["custom", "multi\nline", "a" * rng.randrange(1, 40)])),
]


def random_repr_value(rng, depth=0):
    if depth <= 0:
        return rng.choice(REPR_OBJECTS)(rng)
    kind = rng.randrange(10)
    if kind < 3:
        return [random_repr_value(rng, depth - 1) for _ in range(rng.randrange(4))]
    if kind == 3:
        return tuple(random_repr_value(rng, depth - 1) for _ in range(rng.randrange(4)))
    if kind < 6:
        return {
            rng.choice(["a", "b", 1, 1.5, None, "é"]): random_repr_value(rng, depth - 1)
            for _ in range(rng.randrange(4))
        }
    return rng.choice(REPR_OBJECTS)(rng)


def test_saferepr_20k_combinazioni():
    rng = random.Random(SEED)
    values = [random_repr_value(rng, rng.randrange(4)) for _ in range(20_000)]
    for value in values:
        for maxlen in (None, 0, 1, 5, 64, 1024):
            for maxlevels in (0, 1, 3, 10):
                expected = ref_saferepr(value, maxlen, maxlevels)
                got = rs.saferepr(value, maxlen, maxlevels)
                assert got == expected, (value, maxlen, maxlevels)


def test_saferepr_ricorsione_e_troncamento():
    cycle = []
    cycle.append(cycle)
    mapping = {}
    mapping["x"] = mapping
    values = [
        cycle,
        mapping,
        [1, [2, [3, [4, [5]]]]],
        {"a": [1, {"b": (2,)}]},
        (1,),
        set(),
        frozenset(),
        b"abcdef",
        "abc def",
    ]
    for value in values:
        for maxlen in (None, 0, 1, 2, 3, 5, 64):
            for maxlevels in (0, 1, 3, 10):
                assert rs.saferepr(value, maxlen, maxlevels) == ref_saferepr(
                    value, maxlen, maxlevels
                ), (value, maxlen, maxlevels)


def build_deep_list(depth):
    root = []
    cur = root
    for _ in range(depth):
        child = []
        cur.append(child)
        cur = child
    return root


def test_saferepr_profondita():
    deep = build_deep_list(10_000)
    assert rs.saferepr(deep, None, 0) == ref_saferepr(deep, None, 0)

    # 200.000 livelli: il riferimento è troppo lento, verifichiamo solo che
    # l'implementazione Rust non esaurisca lo stack nativo.
    deeper = build_deep_list(200_000)
    text = rs.saferepr(deeper, None, 0)
    assert len(text) == 2 * (200_000 + 1)
    assert text.startswith("[[")


def test_saferepr_maxlevels_none():
    """`maxlevels=None` (task_repr_maxlevels illimitato) come il riferimento."""
    values = [
        [1, [2, [3, [4, [5]]]]],
        {"a": ["b", {"c": "d"}]},
        {"x": {"a": ["b", {"c": "d"}]}},
    ]
    for value in values:
        for maxlen in (None, 64):
            assert rs.saferepr(value, maxlen, None) == ref_saferepr(
                value, maxlen, None
            )


def test_saferepr_chiavi_annidate():
    for depth in (10, 100, 300):
        cur = HashableDict({1: 0})
        for _ in range(depth):
            cur = HashableDict({cur: 0})
        root = {cur: 1}
        assert rs.saferepr(root, None, 0) == ref_saferepr(root, None, 0)

    # Molto in profondità il riferimento solleva RecursionError: non dobbiamo
    # andare in stack overflow nativo.
    cur = HashableDict({1: 0})
    for _ in range(5000):
        cur = HashableDict({cur: 0})
    with pytest.raises(RecursionError):
        rs.saferepr({cur: 1}, None, 0)


def test_saferepr_stringhe_lunghe():
    rng = random.Random(SEED)
    for _ in range(200):
        text = random_string(rng)
        for maxlen in (64, 1024):
            assert rs.saferepr(text, maxlen) == ref_saferepr(text, maxlen)


def test_json_profondita():
    deep = build_deep_list(10_000)
    with pytest.raises(RecursionError) as ref_err:
        kjson.dumps(deep)
    with pytest.raises(RecursionError) as got_err:
        rs.json_dumps(deep, DEFAULT)
    assert str(got_err.value) == str(ref_err.value)

    # Qualsiasi profondità: valore o RecursionError, mai crash nativo.
    for depth in (2_000, 20_000, 200_000):
        deep = build_deep_list(depth)
        try:
            rs.json_dumps(deep, DEFAULT)
        except RecursionError:
            pass

    for depth in (10_000, 200_000):
        encoded = "[" * depth + "]" * depth
        try:
            value = rs.json_loads(encoded, kjson.object_hook)
        except RecursionError:
            continue
        seen = 0
        while isinstance(value, list):
            seen += 1
            if not value:
                break
            value = value[0]
        assert seen == depth


def build_deep_dict(depth):
    root = {}
    cur = root
    for _ in range(depth):
        child = {}
        cur["x"] = child
        cur = child
    return root


def depth_threshold(serialize, make):
    def works(depth):
        try:
            serialize(make(depth))
            return True
        except RecursionError:
            return False

    lo, hi = 1, 20_000
    while lo < hi:
        mid = (lo + hi + 1) // 2
        if works(mid):
            lo = mid
        else:
            hi = mid - 1
    return lo


def threshold_in_fresh_thread(serialize, make, stack_size=None):
    """Misura la soglia in un thread nuovo.

    Il contatore C di CPython è per-thread: un thread nuovo parte dallo stesso
    budget del riferimento standalone, mentre il thread di pytest può averne
    già consumato una parte.
    """

    result = []

    def worker():
        try:
            result.append(("ok", depth_threshold(serialize, make)))
        except BaseException as exc:  # noqa: BLE001 - riporta al thread chiamante
            result.append(("err", type(exc).__name__, str(exc)))

    old_size = None
    if stack_size is not None:
        old_size = threading.stack_size(stack_size)
    try:
        thread = threading.Thread(target=worker)
        thread.start()
        thread.join()
    finally:
        if old_size is not None:
            threading.stack_size(old_size)

    assert result and result[0][0] == "ok", result
    return result[0][1]


def test_json_dumps_soglia_profondita():
    ref_list = threshold_in_fresh_thread(kjson.dumps, build_deep_list)
    rs_list = threshold_in_fresh_thread(
        lambda value: rs.json_dumps(value, DEFAULT), build_deep_list
    )
    assert rs_list == ref_list == 9_997

    ref_dict = threshold_in_fresh_thread(kjson.dumps, build_deep_dict)
    rs_dict = threshold_in_fresh_thread(
        lambda value: rs.json_dumps(value, DEFAULT), build_deep_dict
    )
    assert rs_dict == ref_dict == 9_997

    with pytest.raises(RecursionError) as got_err:
        rs.json_dumps(build_deep_list(9_998), DEFAULT)
    assert str(got_err.value) == (
        "maximum recursion depth exceeded while encoding a JSON object"
    )


def test_json_dumps_thread_stack_piccolo():
    deep_list = build_deep_list(9_997)
    deep_dict = build_deep_dict(9_997)
    results = []

    def worker():
        try:
            results.append(
                (
                    len(rs.json_dumps(deep_list, DEFAULT)),
                    len(rs.json_dumps(deep_dict, DEFAULT)),
                )
            )
        except BaseException as exc:  # noqa: BLE001 - riporta al thread chiamante
            results.append((type(exc).__name__, str(exc)))

    old_size = threading.stack_size(256 * 1024)
    try:
        thread = threading.Thread(target=worker)
        thread.start()
        thread.join()
    finally:
        threading.stack_size(old_size)

    assert results == [(2 * 9_998, 7 * 9_997 + 2)], results


def test_truncate_fixture():
    rng = random.Random(SEED)
    for _ in range(2_000):
        text = random_string(rng)
        maxlen = rng.choice([None, 0, 1, 5, 64])
        assert rs.saferepr(text, maxlen) == ref_saferepr(text, maxlen)
        if maxlen is not None:
            assert ref_truncate(text, maxlen) is not None


# ---------------------------------------------------------------------------
# encode_task_v2
# ---------------------------------------------------------------------------


def make_app():
    return Celery("differential", broker="memory://", backend="cache+memory://")


def test_encode_task_v2_500_combinazioni():
    rng = random.Random(SEED)
    app = make_app()
    headers_options = ["custom_a", "custom_b", "x"]
    for i in range(500):
        args = rng.choice(
            [
                (),
                (1,),
                (rng.randrange(10), random_string(rng)),
                [1, "a", {"x": [1, 2]}],
                tuple(random_value(rng, 2) for _ in range(rng.randrange(3))),
            ]
        )
        kwargs = rng.choice([{}, {"k": 1}, {"s": random_string(rng)}])
        eta = rng.choice([None, "2026-01-01T00:00:00+00:00", "2026-01-01T00:00:00"])
        expires = rng.choice([None, "2026-01-02T00:00:00+00:00"])
        stamped = rng.choice([None, headers_options[: rng.randrange(4)]])
        stamps = (
            {name: random_string(rng) for name in stamped}
            if stamped is not None
            else None
        )
        argsrepr = rng.choice([None, None, "ARGS"])
        kwargsrepr = rng.choice([None, None, "KWARGS"])
        common = dict(
            task_id=f"task-{i}",
            name=rng.choice(["mytask", "other.task"]),
            args=args,
            kwargs=kwargs,
            eta=eta,
            expires=expires,
            group_id=rng.choice([None, "group-1"]),
            group_index=rng.choice([None, 0, 1, 5]),
            retries=rng.randrange(4),
            chord=rng.choice([None, "chord-1"]),
            callbacks=rng.choice([None, [["x", [], {}]]]),
            errbacks=rng.choice([None, [["e", [], {}]]]),
            reply_to=rng.choice([None, "", "reply-queue"]),
            time_limit=rng.choice([None, 10]),
            soft_time_limit=rng.choice([None, 5]),
            create_sent_event=rng.choice([True, False]),
            root_id=rng.choice([None, "root-1"]),
            parent_id=rng.choice([None, "parent-1"]),
            shadow=rng.choice([None, "shadow-name"]),
            chain=rng.choice([None, [["c", [], {}]]]),
            origin="origin@host",
            ignore_result=rng.choice([True, False]),
            argsrepr=argsrepr,
            kwargsrepr=kwargsrepr,
            stamped_headers=stamped,
            replaced_task_nesting=rng.randrange(3),
        )

        ref_kwargs = dict(common)
        ref_kwargs.pop("replaced_task_nesting")
        ref = app.amqp.as_task_v2(
            **ref_kwargs,
            replaced_task_nesting=common["replaced_task_nesting"],
            **(stamps or {}),
        )
        expected_headers = dict(ref.headers)

        kwargs_rs = dict(common)
        kwargs_rs["stamps"] = stamps
        comp = rng.choice([None, None, "zlib"])
        kwargs_rs["compression"] = comp
        kwargs_rs["default"] = DEFAULT
        headers, properties, body, content_type, content_encoding, sent_event = (
            rs.encode_task_v2(**kwargs_rs)
        )

        ref_body = kser.dumps(ref.body, "json")[2]
        if isinstance(ref_body, str):
            ref_body = ref_body.encode("utf-8")

        if comp:
            expected_headers["compression"] = kcomb.compress(ref_body, comp)[1]
            assert content_type == expected_headers["compression"]
            assert kcomb.decompress(body, content_type) == ref_body
        else:
            assert body == ref_body
        assert content_type in ("application/json", "application/x-gzip")
        assert content_encoding == "utf-8"
        assert list(headers) == list(expected_headers), i
        assert headers == expected_headers, i
        assert list(properties) == ["correlation_id", "reply_to"], i
        assert properties == ref.properties, i
        assert sent_event == ref.sent_event, i
        assert (sent_event is None) == (ref.sent_event is None), i


# ---------------------------------------------------------------------------
# compressione
# ---------------------------------------------------------------------------


def available_algorithms():
    names = []
    for alias in ("gzip", "bzip2", "xz", "brotli", "zstd"):
        try:
            kcomb.get_encoder(alias)
        except KeyError:
            continue
        names.append(alias)
    return names


def test_compression_interop():
    rng = random.Random(SEED)
    sizes = [0, 1, 2, 7, 64, 1024, 65_536, 1_048_576]
    for name in available_algorithms():
        for size in sizes:
            payload = rng.randbytes(size)
            rust_body, rust_ct = rs.compress(payload, name)
            assert kcomb.decompress(rust_body, rust_ct) == payload
            py_body, py_ct = kcomb.compress(payload, name)
            assert py_ct == rust_ct
            assert rs.decompress(py_body, py_ct) == payload


def test_compression_nome_sconosciuto():
    with pytest.raises(KeyError):
        rs.compress(b"x", "sconosciuto")
    with pytest.raises(KeyError):
        rs.decompress(b"x", "sconosciuto")


# ---------------------------------------------------------------------------
# micro-benchmark informativo (non è un gate)
# ---------------------------------------------------------------------------


def test_micro_benchmark(capsys):
    app = make_app()
    args = (1, "abc", {"nested": [1, 2, 3]}, 3.14)
    kwargs = {"a": 1, "b": "x"}
    payload = {"args": args, "kwargs": kwargs}
    n = 20_000

    t0 = time.perf_counter()
    for _ in range(n):
        kjson.dumps(payload)
    ref_dumps = time.perf_counter() - t0

    t0 = time.perf_counter()
    for _ in range(n):
        rs.json_dumps(payload, DEFAULT)
    rs_dumps = time.perf_counter() - t0

    def encode(func):
        for _ in range(n):
            func(
                task_id="id",
                name="bench",
                args=args,
                kwargs=kwargs,
                origin="bench@host",
                default=DEFAULT,
            )

    t0 = time.perf_counter()
    encode(lambda **kw: app.amqp.as_task_v2(**kw))
    ref_task = time.perf_counter() - t0
    t0 = time.perf_counter()
    encode(lambda **kw: rs.encode_task_v2(**kw))
    rs_task = time.perf_counter() - t0

    with capsys.disabled():
        print(
            "\nbenchmark (operazioni/s): "
            f"json_dumps ref={n / ref_dumps:,.0f} rs={n / rs_dumps:,.0f}; "
            f"as_task_v2 ref={n / ref_task:,.0f} rs={n / rs_task:,.0f}"
        )
