# BL917 MQTT Bridge

`bl917_mqtt.py` is a Python bridge for the BL-917 / ZhiJinPower solar charge controller.

It connects to the controller over BLE using the Nordic UART Service, reads controller registers using the Modbus-like protocol used by the official app, and publishes the values to MQTT for Home Assistant auto-discovery.

## What It Does

- Connects to the BL-917 over BLE
- Reads the status and config register blocks
- Publishes telemetry to MQTT as JSON
- Publishes Home Assistant MQTT discovery payloads
- Accepts MQTT commands for writable controller settings

The BLE communication is intentionally conservative:

- connect for each poll
- read data
- disconnect again

This matches the controller behavior better than keeping the BLE link open.

## Files

- [bl917_mqtt.py](/w:/Work/bl917-mqtt/bl917_mqtt.py): main bridge script
- [mqtt_config.example.py](/w:/Work/bl917-mqtt/mqtt_config.example.py): public-safe example config
- [references/solar_service.py](/w:/Work/bl917-mqtt/references/solar_service.py): earlier stable reference implementation used during reverse engineering

## Requirements

- Python 3
- BLE support on the host
- MQTT broker

Python packages:

```bash
pip install bleak paho-mqtt
```

## Configuration

The script currently uses constants inside `bl917_mqtt.py` for the controller identity:

- `BLE_ADDRESS`
- `BLE_NAME`

MQTT connection settings are stored separately in `mqtt_config.py`, e.g.:

```python
MQTT_HOST = "127.0.0.1"
MQTT_PORT = 1883
MQTT_USERNAME = None
MQTT_PASSWORD = None
```

If you are publishing the project, keep `mqtt_config.py` private and only publish `mqtt_config.example.py`.

## Running

Edit the BLE and MQTT settings, then run:

```bash
python bl917_mqtt.py
```

The script will:

1. connect to MQTT
2. publish Home Assistant discovery topics
3. poll the controller every `POLL_INTERVAL`
4. publish state updates to MQTT

## MQTT Topics

Base topic:

```text
bl917/<BLE_ADDRESS>/
```

Important topics:

- `bl917/<BLE_ADDRESS>/state`
- `bl917/<BLE_ADDRESS>/availability`
- `bl917/<BLE_ADDRESS>/cmd/load_output/set`
- `bl917/<BLE_ADDRESS>/cmd/cutoff_voltage/set`
- `bl917/<BLE_ADDRESS>/cmd/restore_voltage/set`
- `bl917/<BLE_ADDRESS>/cmd/output_mode/set`

## Home Assistant Entities

The script publishes MQTT discovery for these entities:

- battery voltage
- battery type
- charge current
- load current
- accumulated charge
- controller temperature
- load output switch
- cutoff voltage number
- restore voltage number
- output mode select

## Current Register Understanding

The register map is still partly reverse engineered.

Current working assumptions in the script:

- `0x0001`: battery type mirror
- `0x0002`: battery voltage
- `0x0003`: charge current
- `0x0004`: load current
- `0x0005`: controller temperature
- `0x0009`: accumulated charge
- `0x1001`: battery type
- `0x1005`: output mode
- `0x1007`: load output
- `0x1009`: cutoff voltage in the current script mapping
- `0x100B`: restore voltage in the current script mapping

Some writable registers may still need confirmation against more device testing.

## Battery Type Values

Currently mapped as:

- `1`: lithium
- `2`: gel
- `3`: lead_acid
- `4`: flooded
- `5`: ternary_lithium
- `6`: lifepo4
- `7`: opz

If your controller reports different meanings, adjust `BATTERY_TYPE_MAP` in `bl917_mqtt.py`.

## Notes

- The controller appears to allow only one BLE client at a time.
- The script reads by notifications and clears stale frames before each request for better stability.
- Home Assistant may keep old MQTT discovery entities until they are removed or rediscovered.

## Known Limitations

- The full register map is not completely verified.
- Some units are inferred from testing.
- The controller may disconnect BLE on its own, so polling reliability depends on the host BLE stack.

## Hardware it works with (please complete the list)

Solar Laderegler 12V 24V 30A MPPT High Definition LCD Digital Display Bildschirm Bluetooth Solar Controller Regler
https://de.aliexpress.com/item/1005009244722899.html?spm=a2g0o.order_list.order_list_main.15.2a5a5c5ftlSMbk&gatewayAdapt=glo2deu
