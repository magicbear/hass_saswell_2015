"""Constants for the Saswell 2015 local-broker integration.

This replaces HomePanel's daemon-side `HP_SASWELL` device (see
homepanel's docs/migration/daemon-python-migration.md, "Saswell MQTT
broker" section, and daemon/devices/saswell.py) -- rather than
HomePanel managing these thermostats itself, they're migrated to be
managed directly by Home Assistant, with HomePanel (if it still wants to
see them) reading them back out of Home Assistant via the existing
HP_HomeAssistant bridge.
"""
from __future__ import annotations

DOMAIN = "saswell_2015"
MANUFACTURER = "Saswell"

DEFAULT_HOST = "0.0.0.0"
# The real Saswell 2015 firmware hardcodes 113.106.11.58:1883 as its cloud
# endpoint and can't be reconfigured -- getting real devices to talk to
# this integration relies on DNS/route hijacking that IP to wherever this
# broker actually listens (the user's own network setup, out of scope for
# this integration). Since that hijack can also remap the port via NAT,
# this default is just a convenient default, not a protocol requirement --
# unlike the real cloud endpoint, this port number is free to configure.
DEFAULT_PORT = 1883

SIGNAL_NEW_DEVICE = f"{DOMAIN}_new_device"


def signal_device_update(devid: str) -> str:
    return f"{DOMAIN}_update_{devid}"
