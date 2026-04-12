from __future__ import annotations

import asyncio
import sys
import traceback
import warnings
from collections.abc import Callable, Sequence
from contextlib import AbstractAsyncContextManager
from functools import partial
from typing import TYPE_CHECKING, Any, ParamSpec, TypeVar

import pyodbc

from .cursor import Cursor
from .utils import _ConnectionContextManager, _ContextManager, _is_conn_close_error

if TYPE_CHECKING:
    from asyncio import Future
    from asyncio.events import AbstractEventLoop

__all__ = ["connect", "Connection"]

P = ParamSpec("P")
T = TypeVar("T")


def connect(
    *,
    dsn: str,
    autocommit: bool = False,
    ansi: bool = False,
    timeout: int = 0,
    executor: Any | None = None,
    echo: bool = False,
    after_created: Callable[[pyodbc.Connection], Any] | None = None,
    **kwargs: Any,
) -> _ConnectionContextManager:
    """Accepts an ODBC connection string and returns a new Connection object.

    The connection string can be passed as the string `str`, as a list of
    keywords,or a combination of the two.  Any keywords except autocommit,
    ansi, and timeout are simply added to the connection string.

    param autocommit bool: False or zero, the default, if True or non-zero,
        the connection is put into ODBC autocommit mode and statements are
        committed automatically.
    param ansi bool: By default, pyodbc first attempts to connect using
        the Unicode version of SQLDriverConnectW. If the driver returns IM001
        indicating it does not support the Unicode version, the ANSI version
        is tried.
    param timeout int: An integer login timeout in seconds, used to set
        the SQL_ATTR_LOGIN_TIMEOUT attribute of the connection. The default is
         0  which means the database's default timeout, if any, is use
    param after_created callable: support customize configuration after
        connection is connected.  Must be an async unary function, or leave it
        as None.
    param ansi bool: If True, use the ANSI version of SQLDriverConnectW.
    """
    return _ConnectionContextManager(
        _connect(
            dsn=dsn,
            autocommit=autocommit,
            ansi=ansi,
            timeout=timeout,
            executor=executor,
            echo=echo,
            after_created=after_created,
            **kwargs,
        )
    )


async def _connect(
    *,
    dsn: str,
    autocommit: bool = False,
    ansi: bool = False,
    timeout: int = 0,
    executor: Any | None = None,
    echo: bool = False,
    after_created: Callable[[pyodbc.Connection], Any] | None = None,
    **kwargs: Any,
) -> Connection:
    conn = Connection(
        dsn=dsn,
        autocommit=autocommit,
        ansi=ansi,
        timeout=timeout,
        echo=echo,
        executor=executor,
        after_created=after_created,
        **kwargs,
    )
    await conn._connect()
    return conn


class Connection(AbstractAsyncContextManager):
    """Connection objects manage connections to the database.

    Connections should only be created by the asyncodbc.connect function.
    """

    _source_traceback = None

    def __init__(
        self,
        *,
        dsn: str,
        autocommit: bool = False,
        ansi: bool | None = None,
        timeout: int = 0,
        executor: Any | None = None,
        echo: bool = False,
        after_created: Callable[[pyodbc.Connection], Any] | None = None,
        **kwargs: Any,
    ) -> None:
        self._executor = executor
        self._loop = asyncio.get_event_loop()
        self._conn: pyodbc.Connection | None = None
        self._expired = False
        self._timeout = timeout
        self._last_usage = self._loop.time()
        self._autocommit = autocommit
        self._ansi = ansi
        self._dsn = dsn
        self._echo = echo
        self._posthook = after_created
        self._kwargs: dict[str, Any] = kwargs
        self._connected = False
        if self.loop.get_debug():
            self._source_traceback = traceback.extract_stack(sys._getframe(1))

    def _execute(self, func: Callable[P, T], *args: P.args, **kwargs: P.kwargs) -> Future[T]:
        # execute function with args and kwargs in thread pool
        func = partial(func, *args, **kwargs)
        future = asyncio.get_event_loop().run_in_executor(self._executor, func)
        return future

    async def _connect(self) -> None:
        # create pyodbc connection
        f: Future[pyodbc.Connection] = self._execute(
            pyodbc.connect,
            self._dsn,
            autocommit=self._autocommit,
            ansi=self._ansi,
            timeout=self._timeout,
            **self._kwargs,
        )
        self._conn = await f
        self._connected = True
        if self._posthook is not None:
            await self._posthook(self._conn)

    @property
    def connected(self) -> bool:
        return self._connected

    @property
    def pyodbc_conn(self) -> pyodbc.Connection:
        if self._conn is None:
            raise RuntimeError(f"{self} not inited!")
        return self._conn

    @property
    def expired(self) -> bool:
        return self._expired

    @property
    def loop(self) -> AbstractEventLoop:
        return self._loop

    @property
    def closed(self) -> bool:
        if self._conn:
            return False
        return True

    @property
    def autocommit(self) -> bool:
        """Show autocommit mode for current database session. True if the
        connection is in autocommit mode; False otherwise. The default
        is False
        """
        return self.pyodbc_conn.autocommit

    @property
    def timeout(self) -> int:
        return self.pyodbc_conn.timeout

    @property
    def last_usage(self) -> float:
        return self._last_usage

    @property
    def echo(self) -> bool:
        return self._echo

    async def _cursor(self) -> Cursor:
        c: pyodbc.Cursor = await self._execute(self.pyodbc_conn.cursor)
        self._last_usage = self._loop.time()
        return Cursor(c, self, echo=self._echo)

    def cursor(self) -> _ContextManager[Cursor]:
        return _ContextManager(self._cursor())

    async def close(self) -> None:
        """Close pyodbc connection"""
        if not self._conn:
            return
        await self._execute(self._conn.close)
        self._conn = None

    def commit(self) -> Future[None]:
        """Commit any pending transaction to the database."""
        fut: Future[None] = self._execute(self.pyodbc_conn.commit)
        return fut

    def rollback(self) -> Future[None]:
        """Causes the database to roll back to the start of any pending
        transaction.
        """
        fut: Future[None] = self._execute(self.pyodbc_conn.rollback)
        return fut

    async def execute(self, sql: str, *args: Any) -> Cursor:
        """Create a new Cursor object, call its execute method, and return it.

        See Cursor.execute for more details.This is a convenience method
        that is not part of the DB API.  Since a new Cursor is allocated
        by each call, this should not be used if more than one SQL
        statement needs to be executed.

        :raises pyodbc.Error: When an error is encountered during execution
        """
        try:
            _cursor: Future[pyodbc.Cursor] = await self._execute(
                self.pyodbc_conn.execute, sql, *args
            )
            connection = self
            cursor = Cursor(_cursor, connection, echo=self._echo)
            return cursor
        except pyodbc.Error as e:
            if _is_conn_close_error(e):
                await self.close()
            raise

    def getinfo(self, type_: int) -> Future[Any]:
        """Returns general information about the driver and data source
        associated with a connection by calling SQLGetInfo and returning its
        results. See Microsoft's SQLGetInfo documentation for the types of
        information available.

        :param type_: int, pyodbc.SQL_* constant
        """
        fut: Future[Any] = self._execute(self.pyodbc_conn.getinfo, type_)
        return fut

    def add_output_converter(self, sqltype: int, func: Callable[[bytes | None], Any]) -> Future[None]:
        """Register an output converter function that will be called whenever
        a value with the given SQL type is read from the database.

        :param sqltype: the integer SQL type value to convert, which can
            be one of the defined standard constants (pyodbc.SQL_VARCHAR)
            or a database-specific value (e.g. -151 for the SQL Server 2008
            geometry data type).
        :param func: the converter function which will be called with a
            single parameter, the value, and should return the converted
            value. If the value is NULL, the parameter will be None.
            Otherwise it will be a Python string.
        """
        fut: Future[None] = self._execute(self.pyodbc_conn.add_output_converter, sqltype, func)
        return fut

    def clear_output_converters(self) -> Future[None]:
        """Remove all output converter functions added by
        add_output_converter.
        """
        fut: Future[None] = self._execute(self.pyodbc_conn.clear_output_converters)
        return fut

    def set_attr(self, attr_id: int, value: int) -> Future[None]:
        """Calls SQLSetConnectAttr with the given values.

        param attr_id: the attribute ID (integer) to set. These are ODBC or
            driver constants.
        param value: the connection attribute value to set. At this time
            only integer values are supported.
        """
        fut: Future[None] = self._execute(self.pyodbc_conn.set_attr, attr_id, value)
        return fut

    def __del__(self) -> None:
        if not self.closed:
            # This will block the loop, please use close
            # coroutine to close connection
            if self._conn is not None:
                self._conn.close()
                self._conn = None

            warnings.warn(f"Unclosed connection {self!r}", ResourceWarning, stacklevel=2)

            context = {"connection": self, "message": "Unclosed connection"}
            if self._source_traceback is not None:
                context["source_traceback"] = self._source_traceback
            self._loop.call_exception_handler(context)

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: Any | None,
    ) -> None:
        await self.close()
