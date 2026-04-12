from __future__ import annotations

from collections.abc import Coroutine, Generator, Iterator
from types import TracebackType
from typing import TYPE_CHECKING, Any, Generic, TypeVar

from pyodbc import Error

if TYPE_CHECKING:
    from types import CodeType, FrameType

    from .connection import Connection
    from .pool import Pool


# Issue #195.  Don't pollute the pool with bad conns
# Unfortunately occasionally sqlite will return 'HY000' for invalid query,
# so we need specialize the check
_CONN_CLOSE_ERRORS: dict[str, str | None] = {
    # [Microsoft][ODBC Driver 17 for SQL Server]Communication link failure
    "08S01": None,
    # [HY000] server closed the connection unexpectedly
    "HY000": "[HY000] server closed the connection unexpectedly",
}


def _is_conn_close_error(e: Any) -> bool:
    if not isinstance(e, Error) or len(e.args) < 2:
        return False

    sqlstate, msg = e.args[0], e.args[1]
    if sqlstate not in _CONN_CLOSE_ERRORS:
        return False

    check_msg = _CONN_CLOSE_ERRORS[sqlstate]
    if not check_msg:
        return True

    return msg.startswith(check_msg)


T = TypeVar("T")


class _ContextManager(Coroutine, Generic[T]):
    __slots__ = ("_coro", "_obj")

    def __init__(self, coro: Coroutine[Any, Any, T]) -> None:
        self._coro = coro
        self._obj: T | None = None

    def send(self, value: Any) -> Any:
        return self._coro.send(value)

    def throw(  # type: ignore[override]
        self, typ: type[BaseException], val: object = None, tb: TracebackType | None = None, /
    ) -> Any:
        if val is None:
            return self._coro.throw(typ)
        elif tb is None:
            return self._coro.throw(typ, val)
        else:
            return self._coro.throw(typ, val, tb)

    def close(self) -> None:
        return self._coro.close()

    @property
    def gi_frame(self) -> FrameType | None:
        return self._coro.gi_frame  # type: ignore[attr-defined]

    @property
    def gi_running(self) -> bool:
        return self._coro.gi_running  # type: ignore[attr-defined]

    @property
    def gi_code(self) -> CodeType | None:
        return self._coro.gi_code  # type: ignore[attr-defined]

    def __next__(self) -> Any:
        return self.send(None)

    def __iter__(self) -> Iterator[Any]:
        return self._coro.__await__()

    def __await__(self) -> Generator[Any, Any, T]:
        return self._coro.__await__()

    async def __aenter__(self) -> T:
        self._obj = await self._coro
        return self._obj

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        await self._obj.close()  # type: ignore[union-attr]
        self._obj = None


class _PoolContextManager(_ContextManager["Pool"]):
    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        self._obj.close()  # type: ignore[union-attr]
        await self._obj.wait_closed()  # type: ignore[union-attr]
        self._obj = None


class _PoolAcquireContextManager(_ContextManager["Connection"]):
    __slots__ = ("_coro", "_conn", "_pool")

    def __init__(self, coro: Coroutine[Any, Any, Connection], pool: Pool) -> None:
        super().__init__(coro)
        self._coro = coro
        self._conn: Connection | None = None
        self._pool: Pool | None = pool

    async def __aenter__(self) -> Connection:
        self._conn = await self._coro
        return self._conn

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        try:
            if self._pool is not None and self._conn is not None:
                await self._pool.release(self._conn)
        finally:
            self._pool = None
            self._conn = None


class _ConnectionContextManager(_ContextManager["Connection"]):
    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        await self._obj.close()  # type: ignore[union-attr]
        self._obj = None
