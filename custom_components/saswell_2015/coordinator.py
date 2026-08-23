"""Embedded MQTT-like broker for Saswell 2015 thermostats.

Protocol port of HomePanel's daemon/devices/saswell.py (itself a Python
port of include/cli-pgm/saswell-mqtt.js) -- see that module's docstring
for the full protocol writeup and how it was validated against real
hardware. Reproduced here rather than imported because a HACS custom
component must be self-contained (no dependency on the separate HomePanel
daemon package).

Devices publish under `/{devid}/...`:
  - `/{devid}/version/{value}`, `/{devid}/type/{value}` -- metadata, not a
    "status" field
  - `/{devid}/S00/1/{power},{curTemp},{setTemp},{unk1},{leaveMode}` -- the
    status frame; values live in the TOPIC PATH, not the MQTT payload
    (real firmware publishes with an empty payload body)
  - on a `type` publish, the broker publishes `/{devid}/S00/1/-1` back
    (query nudge, unchanged from the original)
First publish under a device's own topic = "connect" (creates a new
`SaswellDevice` + fires `SIGNAL_NEW_DEVICE`); a broker-level disconnect
marks it unavailable. Outbound commands (`SetPower`/`SetTemperature`/
`SetLeaveMode`) queue for up to 30s if the device hasn't been seen yet,
matching the original's `dev_msg_queue`.
"""
from __future__ import annotations

import logging
import sys
import time
from dataclasses import dataclass, field
from typing import Any, ClassVar

from amqtt.broker import Broker
from amqtt.contexts import BaseContext
from amqtt.plugins.base import BasePlugin
from homeassistant.core import HomeAssistant
from homeassistant.helpers.dispatcher import async_dispatcher_send

from .const import SIGNAL_NEW_DEVICE, signal_device_update

_LOGGER = logging.getLogger(__name__)
_QUEUE_TIMEOUT_S = 30.0


def _to_float(value: str) -> float | None:
    try:
        return float(value)
    except ValueError:
        return None


@dataclass
class SaswellDeviceState:
    devid: str
    ip: str | None = None
    connected: bool = False
    power: bool = False
    current_temperature: float | None = None
    target_temperature: float | None = None
    leave_mode: bool = False
    device_type: str | None = None
    version: str | None = None
    last_update: float = field(default_factory=time.time)


class _GatewayPluginBase(BasePlugin[BaseContext]):
    """amqtt loads plugins by dotted-path string, so there's no
    constructor argument to hand this a coordinator reference --
    `SaswellCoordinator.async_start()` dynamically creates one subclass
    per instance (registered into this module's namespace so amqtt's
    `import_string` loader can find it by name) instead of relying on a
    single shared class attribute, so more than one config entry could
    in principle coexist in the same HA process without clobbering each
    other's state."""

    coordinator: ClassVar[Any] = None

    async def on_broker_client_connected(self, *, client_id: str, client_session: Any) -> None:
        if self.coordinator is not None:
            self.coordinator._client_ip[client_id] = client_session.remote_address

    async def on_broker_client_disconnected(self, *, client_id: str, client_session: Any) -> None:
        if self.coordinator is not None:
            await self.coordinator._handle_disconnect(client_id)

    async def on_broker_message_received(self, *, client_id: str, message: Any) -> None:
        if self.coordinator is not None:
            await self.coordinator._handle_publish(client_id, message)


class SaswellCoordinator:
    def __init__(self, hass: HomeAssistant, host: str, port: int):
        self.hass = hass
        self.host = host
        self.port = port
        self.devices: dict[str, SaswellDeviceState] = {}
        self._known: set[str] = set()
        self._client_ip: dict[str, str | None] = {}
        self._pending: dict[str, list[tuple[float, dict[str, Any]]]] = {}
        self.broker: Broker | None = None
        self._plugin_cls: type | None = None

    async def async_start(self) -> None:
        plugin_cls = type(f"_SaswellPlugin_{id(self)}", (_GatewayPluginBase,), {"coordinator": self})
        setattr(sys.modules[__name__], plugin_cls.__name__, plugin_cls)
        self._plugin_cls = plugin_cls
        plugin_path = f"{__name__}.{plugin_cls.__name__}"

        config = {
            "listeners": {"default": {"type": "tcp", "bind": f"{self.host}:{self.port}"}},
            "plugins": {
                # Real Saswell firmware doesn't authenticate.
                "amqtt.plugins.authentication.AnonymousAuthPlugin": {"allow_anonymous": True},
                plugin_path: {},
            },
        }
        # Broker(config) resolves each plugin's dotted path via
        # importlib.import_module() synchronously in its constructor --
        # HA's event-loop blocking-call detector flags that. Broker()
        # itself can't be built off-thread (its __init__ calls
        # asyncio.get_running_loop(), which needs an actual running loop
        # on the calling thread -- moving construction to
        # async_add_executor_job's worker thread throws "no running event
        # loop", confirmed by testing against the real HA instance this
        # was built against). The best available fix is pre-importing the
        # plugin module on a worker thread first, so Broker()'s own
        # import_module() call on the event loop thread is a fast
        # sys.modules cache hit rather than a real filesystem/import
        # operation.
        await self.hass.async_add_executor_job(__import__, "amqtt.plugins.authentication")
        self.broker = Broker(config)
        await self.broker.start()
        _LOGGER.info("Saswell broker listening on %s:%s", self.host, self.port)

    async def async_stop(self) -> None:
        if self.broker is not None:
            await self.broker.shutdown()
            self.broker = None
        if self._plugin_cls is not None:
            delattr(sys.modules[__name__], self._plugin_cls.__name__)
            self._plugin_cls = None

    # ---- outbound ----

    async def async_send_command(self, devid: str, topic: str, payload: str | None) -> None:
        if topic in ("SetPower", "sw"):
            out_topic, out_payload = f"/{devid}/S01/1/{'0' if payload in ('0', None) else '1'}", None
        elif topic == "SetTemperature":
            out_topic, out_payload = f"/{devid}/S02/1/{payload}", None
        elif topic == "SetLeaveMode":
            out_topic, out_payload = f"/{devid}/S03/1/{'0' if payload in ('0', None) else '1'}", None
        else:
            out_topic, out_payload = topic, payload

        if devid not in self._known:
            self._pending.setdefault(devid, []).append(
                (time.time() + _QUEUE_TIMEOUT_S, {"topic": out_topic, "payload": out_payload})
            )
            return
        await self._publish(devid, out_topic, out_payload)

    async def _flush_pending(self, devid: str) -> None:
        now = time.time()
        for expires_at, packet in self._pending.pop(devid, []):
            if expires_at >= now:
                await self._publish(devid, packet["topic"], packet["payload"])

    async def _publish(self, devid: str, topic: str, payload: str | None) -> None:
        if self.broker is None:
            return
        data = (payload or "").encode("utf-8")
        await self.broker.plugins_manager.context.broadcast_message(topic, data, qos=1)

    # ---- inbound ----

    async def _handle_disconnect(self, devid: str) -> None:
        self._known.discard(devid)
        self._client_ip.pop(devid, None)
        state = self.devices.get(devid)
        if state is not None:
            state.connected = False
            state.last_update = time.time()
            async_dispatcher_send(self.hass, signal_device_update(devid))

    async def _handle_publish(self, client_id: str, message: Any) -> None:
        devid = client_id
        topic = message.topic
        prefix = f"/{devid}/"
        if not topic.startswith(prefix):
            return

        first_seen = devid not in self._known
        if first_seen:
            self._known.add(devid)

        state = self.devices.get(devid)
        is_new_device = state is None
        if state is None:
            state = SaswellDeviceState(devid=devid)
            self.devices[devid] = state

        rest = topic[len(prefix) :].split("/")
        field_name = rest[0]

        if first_seen:
            state.ip = self._client_ip.get(devid)
            state.connected = True

        if field_name in ("version", "type") and len(rest) > 1:
            setattr(state, "version" if field_name == "version" else "device_type", rest[1])
        elif field_name == "S00" and len(rest) > 2:
            parts = rest[2].split(",")
            if len(parts) >= 5:
                state.power = parts[0] == "1"
                state.current_temperature = _to_float(parts[1])
                state.target_temperature = _to_float(parts[2])
                state.leave_mode = parts[4] == "1"

        state.last_update = time.time()

        if is_new_device:
            async_dispatcher_send(self.hass, SIGNAL_NEW_DEVICE, devid)
        else:
            async_dispatcher_send(self.hass, signal_device_update(devid))

        if first_seen:
            await self._flush_pending(devid)

        if field_name == "type":
            await self._publish(devid, f"/{devid}/S00/1/-1", None)
