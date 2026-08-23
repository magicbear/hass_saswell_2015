"""Config flow for Saswell 2015 local-broker integration."""
from __future__ import annotations

import asyncio
import logging
from typing import Any

import voluptuous as vol

from homeassistant.config_entries import ConfigFlow
from homeassistant.const import CONF_HOST, CONF_PORT
from homeassistant.data_entry_flow import FlowResult

from .const import DEFAULT_HOST, DEFAULT_PORT, DOMAIN

_LOGGER = logging.getLogger(__name__)

_STEP_USER_SCHEMA = vol.Schema(
    {
        vol.Required(CONF_HOST, default=DEFAULT_HOST): str,
        vol.Required(CONF_PORT, default=DEFAULT_PORT): vol.Coerce(int),
    }
)


async def _can_bind(host: str, port: int) -> str | None:
    """Returns an error code, or None if the host:port is bindable.

    There's nothing to "connect to" and verify here (the whole point is a
    broker devices connect TO), so the only meaningful pre-flight check is
    whether the port is actually free to bind."""
    loop = asyncio.get_running_loop()
    try:
        server = await loop.create_server(asyncio.Protocol, host=host, port=port)
    except OSError:
        return "cannot_bind"
    server.close()
    await server.wait_closed()
    return None


class SaswellConfigFlow(ConfigFlow, domain=DOMAIN):
    VERSION = 1

    async def async_step_user(self, user_input: dict[str, Any] | None = None) -> FlowResult:
        errors: dict[str, str] = {}
        if user_input is not None:
            self._async_abort_entries_match(
                {CONF_HOST: user_input[CONF_HOST], CONF_PORT: user_input[CONF_PORT]}
            )
            error = await _can_bind(user_input[CONF_HOST], user_input[CONF_PORT])
            if error:
                errors["base"] = error
            else:
                return self.async_create_entry(
                    title=f"Saswell ({user_input[CONF_HOST]}:{user_input[CONF_PORT]})",
                    data=user_input,
                )

        return self.async_show_form(step_id="user", data_schema=_STEP_USER_SCHEMA, errors=errors)
