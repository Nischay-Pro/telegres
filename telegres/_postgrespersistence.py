#!/usr/bin/env python
#
# A library that extends the functionality of Python Telegram Bot
# by providing more Persistence classes.
# Copyright (C) 2022
# Nischay Ram Mamidi <NischayPro@protonmail.com>
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Lesser Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Lesser Public License for more details.
#
# You should have received a copy of the GNU Lesser Public License
# along with this program.  If not, see [http://www.gnu.org/licenses/].

"""This module contains the PostgresPersistence class."""
from copy import deepcopy
from typing import Any, Dict, Optional, Tuple, cast, overload
import psycopg

try:
    import ujson as json
except ImportError:
    import json  # type: ignore[no-redef]  # noqa: F723
from urllib.parse import urlparse
from tenacity import retry, wait_exponential, stop_after_attempt

from telegram.ext import BasePersistence, PersistenceInput
from telegram.ext._contexttypes import ContextTypes
from telegram.ext._utils.types import BD, CD, UD, CDCData, ConversationDict, ConversationKey


class PostgresPersistence(BasePersistence[UD, CD, BD]):
    """Using psycopg's Postgres connector for making your bot persistent.

    Attention:
        The interface provided by this class is intended to be accessed exclusively by
        :class:`~telegram.ext.Application`. Calling any of the methods below manually might
        interfere with the integration of persistence into :class:`~telegram.ext.Application`.

    Args:
        store_data (:class:`PersistenceInput`, optional): Specifies which kinds of data will be
            saved by this persistence instance. By default, all available kinds of data will be
            saved.
        on_flush (:obj:`bool`, optional): When :obj:`True` will only save to file when
            :meth:`flush` is called and keep data in memory until that happens. When
            :obj:`False` will store data on any transaction *and* on call to :meth:`flush`.
            Default is :obj:`False`.
        context_types (:class:`telegram.ext.ContextTypes`, optional): Pass an instance
            of :class:`telegram.ext.ContextTypes` to customize the types used in the
            ``context`` interface. If not passed, the defaults documented in
            :class:`telegram.ext.ContextTypes` will be used.
        update_interval (:obj:`int` | :obj:`float`, optional): The
            :class:`~telegram.ext.Application` will update
            the persistence in regular intervals. This parameter specifies the time (in seconds) to
            wait between two consecutive runs of updating the persistence. Defaults to 30 seconds.
        postgres_database (:obj:`str`, optional): The name of the database to use. Defaults to `telegres`.
        postgres_username (:obj:`str`, optional): The username to use for connecting to the database.
            Defaults to `telegres`.
        postgres_password (:obj:`str`, optional): The password to use for connecting to the database.
            Defaults to `password123`.
        postgres_host (:obj:`str`, optional): The hostname of the database. Defaults to "localhost".
        postgres_port (:obj:`int`, optional): The port of the database. Defaults to 5432.
        postgres_schema (:obj:`str`, optional): The schema to use. Defaults to "default".
        postgres_url (:obj:`str`, optional): The URL of the database. Defaults to None.
        postgres_timestamp (:obj:`bool`, optional): When :obj:`True` will add the timestamp
            column to store the created_at and updated_at timestamps. Defaults to :obj:`True`.


    Attributes:
        store_data (:class:`PersistenceInput`): Specifies which kinds of data will be saved by this
            persistence instance.
        on_flush (:obj:`bool`, optional): When :obj:`True` will only save to file when
            :meth:`flush` is called and keep data in memory until that happens. When
            :obj:`False` will store data on any transaction *and* on call to :meth:`flush`.
            Default is :obj:`False`.
        context_types (:class:`telegram.ext.ContextTypes`): Container for the types used
            in the ``context`` interface.
    """

    __slots__ = (
        "postgres_database",
        "postgres_username",
        "postgres_password",
        "postgres_host",
        "postgres_port",
        "postgres_schema",
        "postgres_cursor",
        "postgres_connection",
        "postgres_url",
        "postgres_timestamp",
        "on_flush",
        "user_data",
        "chat_data",
        "bot_data",
        "update_interval",
        "callback_data",
        "conversations",
        "context_types",
    )

    @overload
    def __init__(
        self: "PostgresPersistence[Dict, Dict, Dict]",
        postgres_url: str,
        postgres_schema: str,
        postgres_timestamp: bool = True,
        store_data: PersistenceInput = None,
        on_flush: bool = False,
        update_interval: float = 30,
    ):
        ...

    @overload
    def __init__(
        self: "PostgresPersistence[Dict, Dict, Dict]",
        postgres_database: str,
        postgres_username: str,
        postgres_password: str,
        postgres_host: str,
        postgres_port: int,
        postgres_schema: str,
        postgres_timestamp: bool = True,
        store_data: PersistenceInput = None,
        on_flush: bool = False,
        update_interval: float = 30,
    ):
        ...

    @overload
    def __init__(
        self: "PostgresPersistence[UD, CD, BD]",
        postgres_database: str,
        postgres_username: str,
        postgres_password: str,
        postgres_host: str,
        postgres_port: int,
        postgres_schema: str,
        postgres_timestamp: bool = True,
        store_data: PersistenceInput = None,
        on_flush: bool = False,
        update_interval: float = 30,
        context_types: ContextTypes[Any, UD, CD, BD] = None,
    ):
        ...

    def __init__(
        self,
        postgres_database: str = "telegres",
        postgres_username: str = "telegres",
        postgres_password: str = "password123",
        postgres_host: str = "localhost",
        postgres_port: int = 5432,
        postgres_schema: str = "default",
        postgres_url: str = None,
        postgres_timestamp: bool = True,
        store_data: PersistenceInput = None,
        on_flush: bool = False,
        update_interval: float = 30,
        context_types: ContextTypes[Any, UD, CD, BD] = None,
    ):
        super().__init__(store_data=store_data, update_interval=update_interval)

        if postgres_url is not None:
            postgres_parsed = urlparse(postgres_url)
            postgres_database = postgres_parsed.path[1:]
            postgres_username = postgres_parsed.username
            postgres_password = postgres_parsed.password
            postgres_host = postgres_parsed.hostname
            postgres_port = postgres_parsed.port

        self.postgres_database = postgres_database
        self.postgres_username = postgres_username
        self.postgres_password = postgres_password
        self.postgres_host = postgres_host
        self.postgres_port = postgres_port
        self.postgres_schema = postgres_schema
        self.postgres_connection = None
        self.postgres_cursor = None
        self.postgres_timestamp = postgres_timestamp
        self.on_flush = on_flush
        self.user_data: Optional[Dict[int, UD]] = None
        self.chat_data: Optional[Dict[int, CD]] = None
        self.bot_data: Optional[BD] = None
        self.callback_data: Optional[CDCData] = None
        self.conversations: Optional[Dict[str, Dict[Tuple, object]]] = None
        self.update_interval = update_interval
        self.context_types = cast(ContextTypes[Any, UD, CD, BD], context_types or ContextTypes())

    def __del__(self):
        if self.postgres_connection:
            try:
                self.postgres_connection.close()
            except Exception:
                pass

    def _connect_to_db(self) -> None:
        try:
            connection_string = "dbname={0} user={1} host={2} password={3} port={4}".format(
                self.postgres_database,
                self.postgres_username,
                self.postgres_host,
                self.postgres_password,
                self.postgres_port,
            )
            self.postgres_connection = psycopg.connect(connection_string)
            self.postgres_cursor = self.postgres_connection.cursor()

        except psycopg.OperationalError as exc:
            raise TypeError(f"Failed to connect to Postgres database: {exc}")

    @retry(stop=stop_after_attempt(3), wait=wait_exponential())
    def _check_table(self, table) -> Any:
        sql_command = (
            "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_schema = %s"
            " AND table_name = %s);"
        )
        if self.postgres_connection:
            try:
                res = self.postgres_cursor.execute(
                    sql_command, (self.postgres_schema, f"telegram_{table}")
                ).fetchone()[0]
                return res
            except psycopg.OperationalError as exc:
                self._connect_to_db()
                raise TypeError(f"Failed to check table: {exc}")
        return False

    @retry(stop=stop_after_attempt(3), wait=wait_exponential())
    def _check_schema(self) -> Any:
        sql_command = (
            "SELECT EXISTS(SELECT 1 FROM information_schema.schemata WHERE schema_name = %s);"
        )
        if self.postgres_connection:
            try:
                res = self.postgres_cursor.execute(
                    sql_command, (self.postgres_schema,)
                ).fetchone()[0]
                return res
            except psycopg.OperationalError as exc:
                self._connect_to_db()
                raise TypeError(f"Failed to check table: {exc}")
        return False

    @retry(stop=stop_after_attempt(3), wait=wait_exponential())
    def _create_schema(self) -> None:
        sql_command = "CREATE SCHEMA IF NOT EXISTS {0};".format(self.postgres_schema)
        if self.postgres_connection:
            try:
                self.postgres_cursor.execute(sql_command)
                self.postgres_connection.commit()
            except psycopg.OperationalError as exc:
                self._connect_to_db()
                raise TypeError(f"Failed to create schema: {exc}")

    @retry(stop=stop_after_attempt(3), wait=wait_exponential())
    def _create_timestamp_trigger(self, table) -> None:
        sql_function = """
        CREATE OR REPLACE FUNCTION trigger_set_timestamp()
        RETURNS TRIGGER AS $$
        BEGIN
            IF row(NEW.*) IS DISTINCT FROM row(OLD.*) THEN
                NEW.updated_at = now(); 
                RETURN NEW;
            ELSE
                RETURN OLD;
            END IF;
        END;
        $$ language 'plpgsql';
        """
        sql_trigger = (
            "CREATE TRIGGER set_timestamp BEFORE UPDATE ON {0}.telegram_{1} FOR EACH ROW "
            "EXECUTE PROCEDURE trigger_set_timestamp();".format(self.postgres_schema, table)
        )
        try:
            self.postgres_cursor.execute(sql_function)
            self.postgres_cursor.execute(sql_trigger)
        except psycopg.OperationalError as exc:
            self._connect_to_db()
            raise TypeError(f"Failed to create timestamp trigger: {exc}")

    @retry(stop=stop_after_attempt(3), wait=wait_exponential())
    def _create_table(self, table) -> None:
        if self.postgres_timestamp:
            sql_command = (
                "CREATE TABLE IF NOT EXISTS {0}.telegram_{1} "
                "(id BIGINT NOT NULL PRIMARY KEY, data jsonb NOT NULL, "
                "created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(), "
                "updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW());".format(
                    self.postgres_schema, table
                )
            )
        else:
            sql_command = (
                "CREATE TABLE IF NOT EXISTS {0}.telegram_{1} "
                "(id BIGINT NOT NULL PRIMARY KEY, data jsonb NOT NULL);".format(
                    self.postgres_schema, table
                )
            )
        if self.postgres_connection:
            try:
                self.postgres_cursor.execute(sql_command)
                if self.postgres_timestamp:
                    self._create_timestamp_trigger(table)
                self.postgres_connection.commit()
            except psycopg.OperationalError as exc:
                self._connect_to_db()
                raise TypeError(f"Failed to create table: {exc}")

    @retry(stop=stop_after_attempt(3), wait=wait_exponential())
    def _check_key_in_table(self, table, id) -> bool:
        if self.postgres_connection:
            try:
                sql_command = (
                    "SELECT EXISTS (SELECT 1 FROM {0}.telegram_{1} WHERE id = %s);".format(
                        self.postgres_schema, table
                    )
                )
                res = self.postgres_cursor.execute(sql_command, (id,)).fetchone()[0]
                return res
            except psycopg.OperationalError as exc:
                self._connect_to_db()
                raise TypeError(f"Failed to check id exists in table: {exc}")

    @retry(stop=stop_after_attempt(3), wait=wait_exponential())
    def _dump_table_to_db(self, table, data) -> Any:
        try:
            if self.postgres_connection:
                for unique_id in data.keys():
                    data_dump = json.dumps(data[unique_id])
                    if not self._check_key_in_table(table, unique_id):
                        sql_command = (
                            "INSERT INTO {0}.telegram_{1} (id, data) VALUES (%s, %s);".format(
                                self.postgres_schema, table
                            )
                        )
                        self.postgres_cursor.execute(
                            sql_command, (unique_id, data_dump)
                        )
                    else:
                        sql_command = (
                            "UPDATE {0}.telegram_{1} SET data = %s WHERE id = %s;".format(
                                self.postgres_schema, table
                            )
                        )
                        self.postgres_cursor.execute(
                            sql_command, (data_dump, unique_id)
                        )
                self.postgres_connection.commit()
        except psycopg.OperationalError as exc:
            self._connect_to_db()
            raise TypeError(f"Failed to dump data to table: {exc}")

    def _dump_to_db(self) -> None:
        if self.user_data:
            self._dump_table_to_db("user", self.user_data)
        if self.chat_data:
            self._dump_table_to_db("chat", self.chat_data)
        if self.bot_data:
            data = {1: self.bot_data}
            self._dump_table_to_db("bot", data)
        if self.callback_data:
            data = {1: self.callback_data}
            self._dump_table_to_db("callback", data)
        if self.conversations:
            data = {1: self.conversations}
            self._dump_table_to_db("conversations", data)

    @retry(stop=stop_after_attempt(3), wait=wait_exponential())
    def _load_table_from_db(self, table) -> Any:
        if not (self.postgres_connection):
            self._connect_to_db()

        if not self._check_schema():
            self._create_schema()

        if not self._check_table(table):
            self._create_table(table)

        try:
            if self.postgres_connection:
                sql_command = "SELECT id, data FROM {0}.telegram_{1};".format(
                    self.postgres_schema, table
                )
                self.postgres_cursor.execute(sql_command)
                res = self.postgres_cursor.fetchall()
                if res:
                    return {row[0]: row[1] for row in res}
        except psycopg.OperationalError as exc:
            self._connect_to_db()
            raise TypeError(f"Failed to load data from table: {exc}")
        return None

    def _load_from_db(self) -> None:
        self.user_data = {}
        self.chat_data = {}
        self.bot_data = self.context_types.bot_data()
        self.callback_data = {}
        self.conversations = {}

        if not self._check_schema():
            self._create_schema()

        if self._check_table("user"):
            self.user_data = self._load_table_from_db("user")
        else:
            self._create_table("user")

        if self._check_table("chat"):
            self.chat_data = self._load_table_from_db("chat")
        else:
            self._create_table("chat")

        if self._check_table("bot"):
            self.bot_data = self._load_table_from_db("bot")
        else:
            self._create_table("bot")

        if self._check_table("callback"):
            self.callback_data = self._load_table_from_db("callback")[1]
        else:
            self._create_table("callback")

        if self._check_table("conversations"):
            self.conversations = self._load_table_from_db("conversations")[1]
        else:
            self._create_table("conversations")

    async def get_user_data(self) -> Dict[int, UD]:
        """Returns the user_data from the user table if it exists or an empty :obj:`dict`.

        Returns:
            Dict[:obj:`int`, :obj:`dict`]: The restored user data.
        """
        if self.user_data:
            pass
        else:
            self.user_data = self._load_table_from_db("user")

            if not self.user_data:
                self.user_data = {}
        return deepcopy(self.user_data)  # type: ignore[arg-type]

    async def get_chat_data(self) -> Dict[int, CD]:
        """Returns the chat_data from the chat table if it exists or an empty :obj:`dict`.

        Returns:
            Dict[:obj:`int`, :obj:`dict`]: The restored chat data.
        """
        if self.chat_data:
            pass
        else:
            self.chat_data = self._load_table_from_db("chat")
            if not self.chat_data:
                self.chat_data = {}
        return deepcopy(self.chat_data)  # type: ignore[arg-type]

    async def get_bot_data(self) -> BD:
        """Returns the bot_data from the bot table if it exists or an empty object of type
        :obj:`dict` | :attr:`telegram.ext.ContextTypes.bot_data`.

        Returns:
            :obj:`dict` | :attr:`telegram.ext.ContextTypes.bot_data`: The restored bot data.
        """
        if self.bot_data:
            pass
        else:
            self.bot_data = self._load_table_from_db("bot")
            if not self.bot_data:
                self.bot_data = self.context_types.bot_data()
            else:
                self.bot_data = self.bot_data[1]
        return deepcopy(self.bot_data)  # type: ignore[return-value]

    async def get_callback_data(self) -> Optional[CDCData]:
        """Returns the callback data from the callback table if it exists or :obj:`None`.

        Returns:
            Optional[Tuple[List[Tuple[:obj:`str`, :obj:`float`, \
                Dict[:obj:`str`, :class:`object`]]], Dict[:obj:`str`, :obj:`str`]]]:
                The restored metadata or :obj:`None`, if no data was stored.
        """
        if self.callback_data:
            pass
        else:
            self.callback_data = self._load_table_from_db("callback")
            if not self.callback_data:
                return None
            else:
                self.callback_data = tuple(self.callback_data[1])
        if self.callback_data is None:
            return None
        return deepcopy(self.callback_data)

    async def get_conversations(self, name: str) -> ConversationDict:
        """Returns the conversations from the conversations table if it exists or an empty dict.

        Args:
            name (:obj:`str`): The handlers name.

        Returns:
            :obj:`dict`: The restored conversations for the handler.
        """
        if self.conversations:
            pass
        else:
            self.conversations = self._load_table_from_db("conversations")
            if not self.conversations:
                self.conversations = {name: {}}
            else:
                self.conversations = self.conversations[1]
        return self.conversations.get(name, {}).copy()  # type: ignore[union-attr]

    async def update_conversation(
        self, name: str, key: ConversationKey, new_state: Optional[object]
    ) -> None:
        """Will update the conversations for the given handler and depending on :attr:`on_flush`
        dump to the conversations table.

        Args:
            name (:obj:`str`): The handler's name.
            key (:obj:`tuple`): The key the state is changed for.
            new_state (:class:`object`): The new state for the given key.
        """
        if not self.conversations:
            self.conversations = {}
        if self.conversations.setdefault(name, {}).get(key) == new_state:
            return
        self.conversations[name][key] = new_state
        if not self.on_flush:
            data = {1: self.conversations}
            self._dump_table_to_db("conversations", data)

    async def update_user_data(self, user_id: int, data: UD) -> None:
        """Will update the user_data and depending on :attr:`on_flush` dump to the user table.

        Args:
            user_id (:obj:`int`): The user the data might have been changed for.
            data (:obj:`dict`): The :attr:`telegram.ext.Application.user_data` ``[user_id]``.
        """
        if self.user_data is None:
            self.user_data = {}
        if self.user_data.get(user_id) == data:
            return
        self.user_data[user_id] = data
        if not self.on_flush:
            self._dump_table_to_db("user", self.user_data)

    async def update_chat_data(self, chat_id: int, data: CD) -> None:
        """Will update the chat_data and depending on :attr:`on_flush` dump to the chat table.

        Args:
            chat_id (:obj:`int`): The chat the data might have been changed for.
            data (:obj:`dict`): The :attr:`telegram.ext.Application.chat_data` ``[chat_id]``.
        """
        if self.chat_data is None:
            self.chat_data = {}
        if self.chat_data.get(chat_id) == data:
            return
        self.chat_data[chat_id] = data
        if not self.on_flush:
            self._dump_table_to_db("chat", self.chat_data)

    async def update_bot_data(self, data: BD) -> None:
        """Will update the bot_data and depending on :attr:`on_flush` dump to the bot table.

        Args:
            data (:obj:`dict` | :attr:`telegram.ext.ContextTypes.bot_data`): The
                :attr:`telegram.ext.Application.bot_data`.
        """
        if self.bot_data == data:
            return
        self.bot_data = data
        if not self.on_flush:
            data = {1: self.bot_data}
            self._dump_table_to_db("bot", data)

    async def update_callback_data(self, data: CDCData) -> None:
        """Will update the callback_data (if changed) and depending on :attr:`on_flush` dump to 
        the callback table.

        Args:
            data (Tuple[List[Tuple[:obj:`str`, :obj:`float`, \
                Dict[:obj:`str`, :class:`object`]]], Dict[:obj:`str`, :obj:`str`]]):
                The relevant data to restore :class:`telegram.ext.CallbackDataCache`.
        """
        if self.callback_data == data:
            return
        self.callback_data = data
        if not self.on_flush:
            data = {1: self.callback_data}
            self._dump_table_to_db("callback", data)

    async def drop_chat_data(self, chat_id: int) -> None:
        """Will delete the specified key from the ``chat_data`` and depending on
        :attr:`on_flush` dump to the chat table.

        Args:
            chat_id (:obj:`int`): The chat id to delete from the persistence.
        """
        if self.chat_data is None:
            return
        self.chat_data.pop(chat_id, None)  # type: ignore[arg-type]

        if not self.on_flush:
            self._dump_table_to_db("chat", self.chat_data)

    async def drop_user_data(self, user_id: int) -> None:
        """Will delete the specified key from the ``user_data`` and depending on
        :attr:`on_flush` dump to the user table.

        Args:
            user_id (:obj:`int`): The user id to delete from the persistence.
        """
        if self.user_data is None:
            return
        self.user_data.pop(user_id, None)  # type: ignore[arg-type]

        if not self.on_flush:
            self._dump_table_to_db("user", self.user_data)

    async def refresh_user_data(self, user_id: int, user_data: UD) -> None:
        """Does nothing.

        .. versionadded:: 13.6
        .. seealso:: :meth:`telegram.ext.BasePersistence.refresh_user_data`
        """

    async def refresh_chat_data(self, chat_id: int, chat_data: CD) -> None:
        """Does nothing.

        .. versionadded:: 13.6
        .. seealso:: :meth:`telegram.ext.BasePersistence.refresh_chat_data`
        """

    async def refresh_bot_data(self, bot_data: BD) -> None:
        """Does nothing.

        .. versionadded:: 13.6
        .. seealso:: :meth:`telegram.ext.BasePersistence.refresh_bot_data`
        """

    async def flush(self) -> None:
        """Will save all data in memory to the database."""
        if (
            self.user_data
            or self.chat_data
            or self.bot_data
            or self.callback_data
            or self.conversations
        ):
            self._dump_to_db()
            self.postgres_connection.commit()
