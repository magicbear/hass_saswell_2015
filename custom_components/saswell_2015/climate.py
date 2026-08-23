"""Climate platform for Saswell 2015 thermostats."""
from __future__ import annotations

import logging

from homeassistant.components.climate import (
    PRESET_AWAY,
    PRESET_NONE,
    ClimateEntity,
    ClimateEntityFeature,
    HVACAction,
    HVACMode,
)
from homeassistant.config_entries import ConfigEntry
from homeassistant.const import ATTR_TEMPERATURE, PRECISION_TENTHS, UnitOfTemperature
from homeassistant.core import HomeAssistant, callback
from homeassistant.helpers.dispatcher import async_dispatcher_connect
from homeassistant.helpers.entity import DeviceInfo
from homeassistant.helpers.entity_platform import AddEntitiesCallback

from .const import DOMAIN, MANUFACTURER, SIGNAL_NEW_DEVICE, signal_device_update
from .coordinator import SaswellCoordinator, SaswellDeviceState

_LOGGER = logging.getLogger(__name__)

_SUPPORT_FLAGS = (
    ClimateEntityFeature.TARGET_TEMPERATURE
    | ClimateEntityFeature.PRESET_MODE
    | ClimateEntityFeature.TURN_ON
    | ClimateEntityFeature.TURN_OFF
)


async def async_setup_entry(
    hass: HomeAssistant, entry: ConfigEntry, async_add_entities: AddEntitiesCallback
) -> None:
    coordinator: SaswellCoordinator = hass.data[DOMAIN][entry.entry_id]
    added: set[str] = set()

    @callback
    def _add_device(devid: str) -> None:
        # Must be @callback: a plain undecorated function handed to the
        # dispatcher gets classified as an "executor" job and run on a
        # worker thread (HA's default assumption for sync callables that
        # might block) -- `async_add_entities()` internally does
        # `asyncio.get_running_loop()`-dependent scheduling and raises
        # "no running event loop" from a worker thread. Confirmed against
        # the real HA instance this was built against, not a guess.
        if devid in added:
            return
        added.add(devid)
        async_add_entities([SaswellClimate(coordinator, devid)])

    for devid in coordinator.devices:
        _add_device(devid)

    entry.async_on_unload(async_dispatcher_connect(hass, SIGNAL_NEW_DEVICE, _add_device))


class SaswellClimate(ClimateEntity):
    _attr_has_entity_name = True
    _attr_name = None
    _attr_should_poll = False
    _attr_supported_features = _SUPPORT_FLAGS
    _attr_temperature_unit = UnitOfTemperature.CELSIUS
    _attr_precision = PRECISION_TENTHS
    _attr_target_temperature_step = 1.0
    _attr_min_temp = 5.0
    _attr_max_temp = 35.0
    _attr_hvac_modes = [HVACMode.OFF, HVACMode.HEAT]
    _attr_preset_modes = [PRESET_NONE, PRESET_AWAY]

    def __init__(self, coordinator: SaswellCoordinator, devid: str) -> None:
        self._coordinator = coordinator
        self._devid = devid
        self._attr_unique_id = f"{DOMAIN}_{devid}"
        self._attr_device_info = DeviceInfo(
            identifiers={(DOMAIN, devid)},
            manufacturer=MANUFACTURER,
            model="Saswell 2015",
            name=f"Saswell {devid}",
        )

    @property
    def _state(self) -> SaswellDeviceState:
        return self._coordinator.devices[self._devid]

    @property
    def available(self) -> bool:
        return self._state.connected

    @property
    def hvac_mode(self) -> HVACMode:
        return HVACMode.HEAT if self._state.power else HVACMode.OFF

    @property
    def hvac_action(self) -> HVACAction:
        if not self._state.power:
            return HVACAction.OFF
        if self.current_temperature is not None and self.target_temperature is not None:
            if self.current_temperature < self.target_temperature:
                return HVACAction.HEATING
            return HVACAction.IDLE
        return HVACAction.HEATING

    @property
    def preset_mode(self) -> str:
        return PRESET_AWAY if self._state.leave_mode else PRESET_NONE

    @property
    def current_temperature(self) -> float | None:
        return self._state.current_temperature

    @property
    def target_temperature(self) -> float | None:
        return self._state.target_temperature

    async def async_added_to_hass(self) -> None:
        self.async_on_remove(
            async_dispatcher_connect(
                self.hass, signal_device_update(self._devid), self._handle_update
            )
        )

    @callback
    def _handle_update(self) -> None:
        self.async_write_ha_state()

    async def async_set_temperature(self, **kwargs) -> None:
        temperature = kwargs.get(ATTR_TEMPERATURE)
        if temperature is None:
            return
        val_str = str(int(temperature)) if temperature == int(temperature) else str(temperature)
        state = self._state
        state.target_temperature = float(temperature)
        self.async_write_ha_state()
        await self._coordinator.async_send_command(self._devid, "SetTemperature", val_str)

    async def async_set_hvac_mode(self, hvac_mode: HVACMode) -> None:
        state = self._state
        state.power = hvac_mode == HVACMode.HEAT
        self.async_write_ha_state()
        await self._coordinator.async_send_command(
            self._devid, "SetPower", "1" if state.power else "0"
        )

    async def async_turn_on(self) -> None:
        await self.async_set_hvac_mode(HVACMode.HEAT)

    async def async_turn_off(self) -> None:
        await self.async_set_hvac_mode(HVACMode.OFF)

    async def async_set_preset_mode(self, preset_mode: str) -> None:
        state = self._state
        state.leave_mode = preset_mode == PRESET_AWAY
        self.async_write_ha_state()
        await self._coordinator.async_send_command(
            self._devid, "SetLeaveMode", "1" if state.leave_mode else "0"
        )
