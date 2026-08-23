# Saswell 2015 (local broker)

Home Assistant custom integration for Saswell "2015-era" WiFi thermostats
(the ones whose firmware hardcodes `113.106.11.58:1883` as a cloud MQTT
endpoint and can't be repointed at a normal broker).

This integration embeds its own MQTT-like broker directly in Home
Assistant (via [`amqtt`](https://pypi.org/project/amqtt/)) and speaks the
device's actual wire protocol (decoded from `saswell-mqtt.js`, the
Node.js script this replaces). Real devices reach it by routing/NAT-ing
the hardcoded cloud IP to wherever you configure this integration to
listen -- the listen port does **not** have to be 1883, only the
target the device's own firmware hardcodes is fixed.

Each thermostat that connects shows up as a `climate` entity
automatically (no per-device config needed) with:

- `hvac_mode`: `off` / `heat` (device power)
- `target_temperature` / `current_temperature`
- `preset_mode`: `none` / `away` (device "leave mode")

## Install

Via HACS (custom repository), or manually copy
`custom_components/saswell_2015` into your Home Assistant `config/custom_components/`.

Then **Settings → Devices & Services → Add Integration → Saswell 2015**,
enter the listen address/port, and route your hijacked Saswell traffic
there.
