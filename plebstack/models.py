from datetime import datetime
from enum import StrEnum, auto
import json
from typing import cast, Literal, Self
from warnings import warn

from glom import glom, Iter
from glom import Literal as Lit
from pydantic import BaseModel, Field, PositiveFloat, PositiveInt, ValidationError
from rich import print as pprint

tickers = ["BTC/EUR"]


class MsgMethods(StrEnum):
    ping = auto()
    subscribe = auto()
    unsubscribe = auto()


class Channels(StrEnum):
    book = auto()
    executions = auto()
    heartbeat = auto()
    instrument = auto()
    ohlc = auto()
    ticker = auto()
    trade = auto()
    status = auto()


class Msg(BaseModel):
    method: Literal[MsgMethods.ping, MsgMethods.subscribe, MsgMethods.unsubscribe]
    req_id: PositiveInt


class Channel(BaseModel):
    channel: Channels
    symbol: list[str] = Field(default_factory=lambda: tickers)


class OHLCReq(Channel):
    channel: Channels = Channels.ohlc
    interval: Literal[1, 5, 15, 30, 60, 240, 1440, 10080, 21600] = 1  # in minutes


class TickerReq(Channel):
    channel: Channels = Channels.ticker


class TradeReq(Channel):
    channel: Channels = Channels.trade


class SubUnsub(Msg):
    params: OHLCReq | TickerReq | TradeReq


class ResponseKind(StrEnum):
    snapshot = auto()
    update = auto()


class Heartbeat(BaseModel):
    channel: Channels


class Response(Heartbeat):
    type: ResponseKind  # noqa: A003; can't help it, API uses this attribute

    @classmethod
    def channel_name(cls) -> Channels:
        return cls.model_fields["channel"].default

    @classmethod
    def select(
        cls, message: str, response_types: list = []  # noqa: B006
    ) -> Self | None:
        """Parse and validate message, for specified response types

        By default, only the calling response type is validated
        """
        if len(response_types) == 0:
            response_types = [cls]
        models = {r.channel_name(): r for r in response_types}
        payload = json.loads(message)

        if (
            "channel" not in payload
            or payload["channel"] == Channels.heartbeat
            or models.get(payload["channel"]) is None
        ):
            return
        try:
            return cls.model_validate(payload)
        except ValidationError as err:
            pprint(err)
            return


class OHLCData(BaseModel):
    # close: PositiveFloat
    # high: PositiveFloat
    # low: PositiveFloat
    # open: PositiveFloat
    symbol: Literal["BTC/EUR"]
    interval_begin: datetime
    trades: PositiveInt
    volume: PositiveFloat
    vwap: PositiveFloat
    interval: Literal[1, 5, 15, 30, 60, 240, 1440, 10080, 21600]
    timestamp: datetime


class OHLCResponse(Response):
    channel: Channels = Channels.ohlc
    data: list[OHLCData]
    timestamp: datetime

    def as_records(self, columns: dict[str, str]) -> list[dict]:
        """Columns to include in a record: {"column_name": "<API attr>"}"""
        if len(self.data) == 0:
            return []

        if unknown := {
            col: columns.pop(col)
            for col, attr in columns.items()
            if attr not in type(self.data[0]).model_fields
        }:
            warn(f"skipping unknown columns: {unknown}", stacklevel=2)

        record_pattern = {
            "type": Lit(self.type),
            "interval": "interval",
            "begin": "interval_begin",
            **columns,
        }
        res = glom(self.data, Iter().map(record_pattern).all())
        return cast(list[dict], res)
