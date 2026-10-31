"""
Logi gatewaya: klucz API nie moze trafic do logow, a linie z dwoch watkow nie moga sie sklejac.

  install(api_key)  - filtr RedactApiKey na loggerach uvicorna + wyciszenie spamu twisted
  log(msg)          - jedna linia na stdout (watek ROS + petla asyncio)
  DropLog           - odrzucone wiadomosci ze zrodel z limitem linii na zrodlo
"""

from __future__ import annotations

import logging
import re
import sys
import threading
import time
from contextlib import suppress
from typing import Dict, Optional

from pydantic import ValidationError

DROP_LOG_EVERY_S = 10.0  # odrzucone wiadomosci: najwyzej linia co tyle na zrodlo (+ liczba pominietych)
UVICORN_LOGGERS = ("uvicorn.error", "uvicorn.access", "uvicorn.asgi")

# nazwa parametru moze miec dowolna litere zakodowana (%61pi_key - Starlette i tak odczyta api_key);
# wielkosc liter tez ignorujemy - nadmiarowe ukrycie nic nie kosztuje. Separator i cudzyslowy tez zakodowane:
# uvicorn loguje sciezke przez quote() ("api_key%3DK", "api_key%3AK", JSON "%22api_key%22%3A%22K")
_API_KEY_RE = re.compile("(?i)(" + "".join(
    f"(?:{re.escape(ch)}|%{ord(ch.lower()):02x}|%{ord(ch.upper()):02x})" for ch in "api_key")
    + r"(?:%22|\")?\s*(?:=|%3d|:|%3a)\s*(?:%22|\")?)[^&\s]*")
# naglowek handshake WS: websockets loguje go na DEBUG jako "< %s: %s" (nazwa i wartosc osobno)
_HEADER_RE = re.compile(r"(?im)(x-api-key:[ \t]*)[^\r\n]*")
# blad parsowania naglowka (websockets, --http h11): wartosc bez nazwy naglowka, wiec ukrywamy kazda
_HDR_ERR_RE = re.compile(r"(invalid HTTP header (?:line|name|value): ).*")


def key_re(key: str) -> Optional[re.Pattern]:
    """Sam klucz w dowolnym miejscu (traceback, pozycja bez etykiety, raw_path na trace): kazdy znak doslownie
    albo jako %XX bajtow UTF-8 w dowolnej wielkosci liter (quote(), .NET UrlEncode, pelne kodowanie),
    albo \\xNN (repr bytes). Krotki klucz zamaskowalby pol logu."""
    if len(key) < 8:
        return None

    def alt(ch: str) -> str:
        b = ch.encode()
        forms = [re.escape(ch), "(?i:" + "".join(f"%{x:02x}" for x in b) + ")"]
        if not ch.isascii():
            forms.append(re.escape("".join(f"\\x{x:02x}" for x in b)))
        return "(?:" + "|".join(forms) + ")"
    return re.compile("".join(alt(ch) for ch in key))


class RedactApiKey(logging.Filter):
    """uvicorn loguje sciezke z query ('"WebSocket /...?api_key=..." [accepted]' na uvicorn.error,
    access log, przy --log-level debug/trace linie handshake WS z naglowkami na uvicorn.error,
    przy trace scope na uvicorn.asgi), wiec wartosc api_key / X-API-Key zamieniamy na ***.
    Do tego tekst wyjatku (exc_info), bledy parsowania naglowkow i kazde wystapienie samego klucza.
    Ramki WS (debug websockets "< %s" i "> %s" - serwer odsyla PING klienta jako PONG, a CLOSE z jego reason)
    tylko jako typ i rozmiar; przy trace reason z websocket.disconnect tylko jako dlugosc.
    Filtr na loggerze przetrwa dictConfig uvicorna, wiec dziala niezaleznie od sposobu uruchomienia."""

    def __init__(self, api_key: str = ""):
        super().__init__()
        self.key_re = key_re(api_key)

    def _redact(self, v, whole: bool = False):
        # whole: gotowy tekst (nie format z %s) - tylko wtedy ucinamy reszte linii bledu naglowka
        if v is None or isinstance(v, (int, float)):
            return v
        s = v if isinstance(v, str) else str(v)  # np. scope (dict) z query_string w bytes
        r = _HEADER_RE.sub(r"\1***", _API_KEY_RE.sub(r"\1***", s))
        if whole:
            r = _HDR_ERR_RE.sub(r"\1***", r)
        if self.key_re is not None:
            r = self.key_re.sub("***", r)
        return v if r == s else r

    def filter(self, record: logging.LogRecord) -> bool:
        a = record.args
        if record.msg in ("< %s", "> %s") and isinstance(a, tuple) and len(a) == 1 and hasattr(a[0], "opcode"):
            # Frame.__str__ przycina srodek tresci - przyciety klucz nie pasuje do zadnej formy, wiec bez tresci
            with suppress(Exception):
                record.args = (f"{a[0].opcode.name} [{len(a[0].data)} bytes]",)
        elif isinstance(a, tuple) and any(isinstance(x, dict) and x.get("type") == "websocket.disconnect" for x in a):
            # trace uvicorn.asgi: reason od klienta jest dowolnym tekstem (moze zawierac obcy klucz)
            record.args = tuple({**x, "reason": f"<{len(x['reason'])} chars>"}
                                if isinstance(x, dict) and x.get("reason") else x for x in a)
        if record.exc_info and not record.exc_text:
            # Formatter uzyje gotowego exc_text zamiast formatowac traceback (np. ValueError z naglowkiem)
            with suppress(Exception):
                record.exc_text = logging.Formatter().formatException(record.exc_info)
        record.exc_text = self._redact(record.exc_text, True)
        record.stack_info = self._redact(record.stack_info, True)
        record.msg = self._redact(record.msg, not record.args)
        if isinstance(record.args, tuple):
            record.args = tuple(self._redact(a) for a in record.args)
        elif isinstance(record.args, dict):
            record.args = {k: self._redact(v) for k, v in record.args.items()}
        if record.args:
            # nazwa naglowka (lub prefiks bledu) i wartosc w osobnych args: sprawdzamy sklejony tekst; access log
            # (args jako krotka dla AccessFormatter) nigdy tu nie wpada - nie ma naglowkow
            with suppress(Exception):
                m = record.getMessage()
                r = _HDR_ERR_RE.sub(r"\1***", _HEADER_RE.sub(r"\1***", m))
                if r != m:
                    record.msg, record.args = r, None
        return True


def install(api_key: str) -> RedactApiKey:
    """Filtr na loggerach uvicorna (przed ich dictConfig - przetrwa) i wyciszenie twisted."""
    f = RedactApiKey(api_key)
    for name in UVICORN_LOGGERS:
        logging.getLogger(name).addFilter(f)
    # roslibpy kieruje logi twisted/autobahn do loggera "twisted"; WARN "dropping connection to peer"
    # leci przy kazdej nieudanej probie (milczacy peer, odpowiedz bez 101) - spam w fazie wolnej
    logging.getLogger("twisted").setLevel(logging.ERROR)
    return f


_log_lock = threading.Lock()


def log(msg: str) -> None:
    """Linia na stdout jednym write + flush pod lockiem: watek ROS i petla asyncio nie skleja linii, a bez
    PYTHONUNBUFFERED (lokalnie, stdout do pliku) linia nie czeka w buforze i nie ginie przy SIGTERM."""
    with _log_lock:
        sys.stdout.write(msg + "\n")
        sys.stdout.flush()


def why(ex: Exception) -> str:
    # jedna linia: ValidationError pydantic to kilka linii z URL, wiec tylko pierwszy blad
    if isinstance(ex, ValidationError):
        errs = ex.errors(include_url=False)
        e = errs[0]
        more = f" (+{len(errs) - 1} more)" if len(errs) > 1 else ""
        return f"{ex.title} {'.'.join(map(str, e['loc']))}: {e['msg']}{more}"
    return repr(ex).replace("\n", " ")[:200]


class DropLog:
    """Odrzucone wiadomosci (NaN, zly format): pierwsza od razu, potem najwyzej linia co DROP_LOG_EVERY_S
    na zrodlo z liczba pominietych - czujnik zawieszony na NaN nie zaleje logow."""

    def __init__(self, prefix: str, every: float = DROP_LOG_EVERY_S, clock=time.monotonic):
        self.prefix, self.every, self.clock = prefix, every, clock
        self._state: Dict[str, tuple] = {}  # zrodlo -> (pominiete, czas ostatniej linii)
        self._lock = threading.Lock()

    def __call__(self, src: str, ex: Exception) -> None:
        now = self.clock()
        with self._lock:
            n, last = self._state.get(src, (0, None))
            if last is not None and now - last < self.every:
                self._state[src] = (n + 1, last)
                return
            self._state[src] = (0, now)
        log(f"{self.prefix} {src}: drop {why(ex)}" + (f" ({n} more dropped since last line)" if n else ""))
