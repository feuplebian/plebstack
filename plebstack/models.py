from dataclasses import fields
from datetime import datetime
from enum import IntEnum, StrEnum, auto
import json
from types import new_class
from typing import cast, Literal, Self
from warnings import warn

from glom import glom, Iter
from glom import Literal as Lit
from pydantic import (
    BaseModel,
    conint,
    Field,
    PositiveFloat,
    PositiveInt,
    RootModel,
    ValidationError,
)
from pydantic.dataclasses import dataclass
from rich import print as pprint

tickers = ["BTC/EUR"]


class MyStrEnum(StrEnum):
    def __repr__(self) -> str:
        return f"{type(self).__name__}(value={self.name!r})"


class MsgMethods(MyStrEnum):
    ping = auto()
    subscribe = auto()
    unsubscribe = auto()


class Channels(MyStrEnum):
    """Channels we may subscribe to."""

    book = auto()
    executions = auto()
    heartbeat = auto()
    instrument = auto()
    ohlc = auto()
    ticker = auto()
    trade = auto()
    status = auto()


class MyIntEnum(IntEnum):
    def __repr__(self) -> str:
        return f"{type(self).__name__}(value={self.value})"


class Intervals(MyIntEnum):
    """Intervals in minutes."""

    min_01 = 1
    min_05 = 5
    min_15 = 15
    min_30 = 30
    hour_1 = 60
    hour_4 = 240
    day_01 = 1440
    day_07 = 10080
    day_15 = 21600


class Msg(BaseModel):
    method: Literal[MsgMethods.ping, MsgMethods.subscribe, MsgMethods.unsubscribe]
    req_id: conint(ge=0)


class Channel(BaseModel):
    channel: Channels
    symbol: list[str] = Field(default_factory=lambda: tickers)


class OHLCReq(Channel):
    channel: Channels = Channels.ohlc
    interval: Intervals = Intervals.min_01


class TickerReq(Channel):
    channel: Channels = Channels.ticker


class TradeReq(Channel):
    channel: Channels = Channels.trade


class SubUnsub(Msg):
    params: OHLCReq | TickerReq | TradeReq


class ResponseKind(MyStrEnum):
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


@dataclass
class OHLCData:
    close: PositiveFloat
    high: PositiveFloat
    low: PositiveFloat
    open: PositiveFloat  # noqa: A003; can't help it, API uses this attribute
    symbol: Literal["BTC/EUR"]
    interval_begin: datetime
    trades: PositiveInt
    volume: PositiveFloat
    vwap: PositiveFloat
    interval: Intervals
    timestamp: datetime


def model_dump_json(model) -> str:
    return RootModel[type(model)](model).model_dump_json()


def make_record_t(name: str, field_map: dict[str, str], annotations: dict[str, type]):
    fields = {
        "type": ResponseKind,
        **{k: annotations[v] for k, v in field_map.items() if v in annotations},
    }
    namespace = {"__annotations__": fields, "model_dump_json": model_dump_json}
    klass = new_class(name, (), {}, lambda ns: ns.update(namespace))
    return dataclass(klass)


class OHLCResponse(Response):
    channel: Channels = Channels.ohlc
    data: list[OHLCData]
    timestamp: datetime

    def as_records(self, columns: dict[str, str]):
        """Columns to include in a record: {"column_name": "<API attr>"}"""
        if len(self.data) == 0:
            return []

        _known = glom(OHLCData, (fields, ["name"]))
        if unknown := {
            col: columns.pop(col) for col, attr in columns.items() if attr not in _known
        }:
            warn(f"skipping unknown columns: {unknown}", stacklevel=2)

        field_map = {
            "type": Lit(self.type),
            "interval": "interval",
            "begin": "interval_begin",
            **columns,
        }
        rec_t = make_record_t("Record", field_map, OHLCData.__annotations__)
        res: list[rec_t] = glom(
            self.data, Iter().map(field_map).map(lambda i: rec_t(**i)).all()
        )
        return res
