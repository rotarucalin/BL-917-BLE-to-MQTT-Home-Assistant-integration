#!/usr/bin/env python3
"""
BL917 BLE -> MQTT bridge for Home Assistant

Requirements:
    python3 -m pip install bleak paho-mqtt

Design:
- Connects fresh for each poll cycle
- Reads both observed register blocks
- Disconnects immediately after read
- Connects fresh for each command write, then reads back state, then disconnects
- Publishes MQTT discovery + state for Home Assistant

This is intentionally conservative because the BL917 appears to drop persistent
BLE links on its own after a few seconds.
"""

from __future__ import annotations

import asyncio
import json
import logging
import signal
import time
from typing import Any, Dict, Optional

from bleak import BleakClient
import paho.mqtt.client as mqtt
from mqtt_config import MQTT_HOST, MQTT_PASSWORD, MQTT_PORT, MQTT_USERNAME


# =========================
# User configuration
# =========================

BLE_ADDRESS = "25:05:00:00:00:FB"
BLE_NAME = "ZJBE-2505000000FB"

MQTT_DISCOVERY_PREFIX = "homeassistant"
MQTT_BASE_TOPIC = f"bl917/{BLE_ADDRESS}"

POLL_INTERVAL = 60.0
RECONNECT_DELAY = 15.0
BLE_RESPONSE_TIMEOUT = 8.0
BLE_CONNECT_TIMEOUT = 8.0
BLE_CONNECT_RETRIES = 3
BLE_SESSION_RETRIES = 3

DEVICE_MANUFACTURER = "ZhiJinPower"
DEVICE_MODEL = "BL-917"

# Nordic UART Service
NOTIFY_UUID = "6e400002-b5a3-f393-e0a9-e50e24dcca9e"
WRITE_UUID = "6e400003-b5a3-f393-e0a9-e50e24dcca9e"

# Modbus unit ID observed in app traffic
UNIT_ID = 0x01

# Register blocks as seen in app traffic
STATUS_START = 0x0001
STATUS_COUNT = 0x0010  # app requests 16 registers; device may return fewer

CONFIG_START = 0x1001
CONFIG_COUNT = 0x000F  # app requests 15 registers; device may return fewer

READ_BLOCKS = [
    (STATUS_START, STATUS_COUNT),
    (CONFIG_START, CONFIG_COUNT),
]

# Inferred writable registers from reverse engineering
REG_LOAD_OUTPUT = 0x1007
REG_CUTOFF_VOLTAGE = 0x1009
REG_RESTORE_VOLTAGE = 0x100B   # inferred, adjust if needed
REG_OUTPUT_MODE = 0x1005       # inferred, adjust if needed

BATTERY_TYPE_MAP = {
    1: "lithium",
    2: "gel",
    3: "lead_acid",
    4: "flooded",
    5: "ternary_lithium",
    6: "lifepo4",
    7: "opz",
}

MAP_TELEMETRY = {
    0x0001: ("battery_type_raw", 1.0),
    0x0002: ("battery_voltage", 0.1),
    0x0003: ("charge_current", 0.1),
    0x0004: ("load_current", 0.1),
    0x0005: ("controller_temperature", 0.01),
    0x0009: ("accumulated_charge", 0.1),
    0x1001: ("battery_type_raw", 1.0),
    0x1007: ("load_output_raw", 1.0),
    0x1009: ("cutoff_voltage", 0.1),
    0x100B: ("restore_voltage", 0.1),
    0x1005: ("output_mode_raw", 1.0),
}

OUTPUT_MODE_MAP = {
    0: "manual",
    1: "24h",
    2: "light",
    3: "timer",
}
OUTPUT_MODE_MAP_REVERSE = {v: k for k, v in OUTPUT_MODE_MAP.items()}

_LOG = logging.getLogger("bl917_mqtt")


# =========================
# Modbus helpers
# =========================

def crc16_modbus(data: bytes) -> bytes:
    crc = 0xFFFF
    for b in data:
        crc ^= b
        for _ in range(8):
            if crc & 1:
                crc = (crc >> 1) ^ 0xA001
            else:
                crc >>= 1
    return crc.to_bytes(2, "little")


def verify_modbus_crc(frame: bytes) -> bool:
    if len(frame) < 4:
        return False
    return crc16_modbus(frame[:-2]) == frame[-2:]


def modbus_read(unit: int, start_reg: int, count: int) -> bytes:
    frame = bytes([
        unit,
        0x03,
        (start_reg >> 8) & 0xFF,
        start_reg & 0xFF,
        (count >> 8) & 0xFF,
        count & 0xFF,
    ])
    return frame + crc16_modbus(frame)


def modbus_write_single_via_fc16(unit: int, reg: int, value: int) -> bytes:
    frame = bytes([
        unit,
        0x10,
        (reg >> 8) & 0xFF,
        reg & 0xFF,
        0x00, 0x01,  # quantity = 1 register
        0x02,        # byte count
        (value >> 8) & 0xFF,
        value & 0xFF,
    ])
    return frame + crc16_modbus(frame)


# =========================
# BLE transport
# =========================

class BL917Client:
    def __init__(self, address: str):
        self.address = address
        self.client: Optional[BleakClient] = None
        self._connected = False
        self._disconnect_expected = False
        self._rx_queue: asyncio.Queue[bytes] = asyncio.Queue()

    def is_connected(self) -> bool:
        return bool(self.client and self.client.is_connected and self._connected)

    async def connect(self) -> None:
        if self.is_connected():
            return

        def _handle_disconnect(_client):
            self._connected = False
            if self._disconnect_expected:
                _LOG.debug("BLE disconnected")
            else:
                _LOG.warning("BLE disconnected")

        last_error: Optional[Exception] = None
        for attempt in range(1, BLE_CONNECT_RETRIES + 1):
            await self.disconnect()
            self._disconnect_expected = False
            self.client = BleakClient(
                self.address,
                timeout=BLE_CONNECT_TIMEOUT,
                disconnected_callback=_handle_disconnect,
            )

            try:
                await self.client.connect()
                if not self.client.is_connected:
                    raise RuntimeError("BLE connect failed")
                self._connected = True
                await asyncio.sleep(0.5)
                await self.client.start_notify(NOTIFY_UUID, self._notification_handler)
                await asyncio.sleep(1.0)
                _LOG.info("BLE connected to %s", self.address)
                return
            except Exception as exc:
                last_error = exc
                _LOG.warning(
                    "BLE connect attempt %d/%d failed: %s",
                    attempt,
                    BLE_CONNECT_RETRIES,
                    exc,
                )
                await self.disconnect()
                if attempt < BLE_CONNECT_RETRIES:
                    await asyncio.sleep(RECONNECT_DELAY)

        raise RuntimeError(
            f"BLE connect failed after {BLE_CONNECT_RETRIES} attempts"
        ) from last_error

    async def disconnect(self) -> None:
        self._connected = False
        self._clear_rx_queue()

        if self.client:
            try:
                if self.client.is_connected:
                    self._disconnect_expected = True
                    try:
                        await self.client.stop_notify(NOTIFY_UUID)
                    except Exception:
                        pass
                    await self.client.disconnect()
            except Exception:
                _LOG.exception("Error during BLE disconnect")
            finally:
                self._disconnect_expected = False
                self.client = None

    def _clear_rx_queue(self) -> None:
        while not self._rx_queue.empty():
            self._rx_queue.get_nowait()

    def _parse_modbus_response(self, frame: bytes) -> Dict[str, Any]:
        if len(frame) < 5:
            raise RuntimeError("Frame too short")
        if not verify_modbus_crc(frame):
            raise RuntimeError(f"Bad CRC in frame: {frame.hex()}")

        unit = frame[0]
        function = frame[1]

        if function & 0x80:
            exc_code = frame[2]
            raise RuntimeError(f"Modbus exception function=0x{function:02x} code=0x{exc_code:02x}")

        if function == 0x03:
            byte_count = frame[2]
            payload = frame[3:3 + byte_count]
            if len(payload) != byte_count:
                raise RuntimeError(
                    f"Malformed response: declared byte_count={byte_count}, actual={len(payload)}"
                )
            if byte_count % 2 != 0:
                raise RuntimeError(f"Malformed response: odd byte_count={byte_count}")

            registers = []
            for i in range(0, byte_count, 2):
                registers.append((payload[i] << 8) | payload[i + 1])
            return {"unit": unit, "func": function, "registers": registers}

        if function == 0x10:
            if len(frame) < 8:
                raise RuntimeError("Write ack too short")
            start_reg = (frame[2] << 8) | frame[3]
            quantity = (frame[4] << 8) | frame[5]
            return {"unit": unit, "func": function, "start_reg": start_reg, "quantity": quantity}

        raise RuntimeError(f"Unsupported function code: 0x{function:02x}")

    async def request(self, frame: bytes, expected_func: int) -> Dict[str, Any]:
        if not self.is_connected():
            raise RuntimeError("BLE not connected")

        for attempt in (1, 2):
            self._clear_rx_queue()
            _LOG.debug("TX attempt=%d frame=%s", attempt, frame.hex())
            await self.client.write_gatt_char(WRITE_UUID, frame, response=False)

            while True:
                try:
                    raw = await asyncio.wait_for(self._rx_queue.get(), timeout=BLE_RESPONSE_TIMEOUT)
                except asyncio.TimeoutError:
                    _LOG.warning("Timeout waiting for response attempt=%d frame=%s", attempt, frame.hex())
                    if not self.is_connected():
                        raise RuntimeError("BLE disconnected")
                    if attempt == 1:
                        await asyncio.sleep(0.5)
                        break
                    raise

                try:
                    parsed = self._parse_modbus_response(raw)
                except Exception as exc:
                    _LOG.warning("Ignoring unparsable frame %s (%s)", raw.hex(), exc)
                    continue

                if parsed["unit"] != UNIT_ID:
                    _LOG.debug("Ignoring response for unexpected unit %s", parsed["unit"])
                    continue

                if parsed["func"] != expected_func:
                    _LOG.debug("Ignoring unexpected function 0x%02X", parsed["func"])
                    continue

                return parsed

        raise RuntimeError("Unreachable request path")

    async def read_registers(self, start_reg: int, count: int, unit: int = UNIT_ID) -> Dict[int, int]:
        if not self.is_connected():
            raise RuntimeError("BLE not connected")

        frame = modbus_read(unit, start_reg, count)
        parsed = await self.request(frame, expected_func=0x03)
        returned_registers = len(parsed["registers"])
        regs: Dict[int, int] = {}
        reg = start_reg
        for value in parsed["registers"]:
            regs[reg] = value
            reg += 1

        _LOG.info(
            "Read response start=0x%04X requested=%d returned=%d",
            start_reg,
            count,
            returned_registers,
        )
        _LOG.debug("RX regs: %s", {f"0x{k:04X}": v for k, v in regs.items()})
        return regs

    async def write_register(self, reg: int, value: int, unit: int = UNIT_ID) -> None:
        if not self.is_connected():
            raise RuntimeError("BLE not connected")

        frame = modbus_write_single_via_fc16(unit, reg, value)
        parsed = await self.request(frame, expected_func=0x10)
        if parsed["start_reg"] != reg or parsed["quantity"] != 1:
            raise RuntimeError(
                f"Write ack mismatch start=0x{parsed['start_reg']:04x} qty={parsed['quantity']}"
            )

    def _notification_handler(self, _sender: int, data: bytearray) -> None:
        frame = bytes(data)
        _LOG.debug("RX notify: %s", frame.hex())
        self._rx_queue.put_nowait(frame)

    async def read_all(self) -> Dict[int, int]:
        if not self.is_connected():
            raise RuntimeError("BLE not connected")

        registers: Dict[int, int] = {}
        for start_reg, count in READ_BLOCKS:
            if not self.is_connected():
                raise RuntimeError("BLE not connected")
            block = await self.read_registers(start_reg, count)
            registers.update(block)
            await asyncio.sleep(0.3)
        return registers


# =========================
# MQTT bridge
# =========================

class BL917MqttBridge:
    def __init__(self, ble_client: BL917Client):
        self.ble = ble_client
        self.mqtt = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=f"bl917-{BLE_ADDRESS}")
        self.loop: Optional[asyncio.AbstractEventLoop] = None
        self.stop_event = asyncio.Event()
        self.ble_op_lock = asyncio.Lock()

        if MQTT_USERNAME:
            self.mqtt.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)

        self.mqtt.on_connect = self._on_mqtt_connect
        self.mqtt.on_message = self._on_mqtt_message
        self.mqtt.on_disconnect = self._on_mqtt_disconnect

    async def start(self) -> None:
        self.loop = asyncio.get_running_loop()
        self.mqtt.connect(MQTT_HOST, MQTT_PORT, keepalive=60)
        self.mqtt.loop_start()

        await self.publish_discovery()

        try:
            while not self.stop_event.is_set():
                sleep_for = POLL_INTERVAL
                try:
                    state = await self.poll_once()
                    self.publish_state(state)
                    self.publish_availability("online")
                except asyncio.CancelledError:
                    raise
                except Exception:
                    _LOG.exception("BLE poll failed")
                    self.publish_availability("offline")
                    sleep_for = RECONNECT_DELAY

                await asyncio.sleep(sleep_for)
        finally:
            self.publish_availability("offline")
            await self.ble.disconnect()
            self.mqtt.loop_stop()
            self.mqtt.disconnect()

    async def poll_once(self) -> dict:
        async with self.ble_op_lock:
            last_error: Optional[Exception] = None
            for attempt in range(1, BLE_SESSION_RETRIES + 1):
                try:
                    await self.ble.connect()
                    regs = await self.ble.read_all()
                    return self.decode_state(regs)
                except Exception as exc:
                    last_error = exc
                    _LOG.warning(
                        "BLE poll session attempt %d/%d failed: %s",
                        attempt,
                        BLE_SESSION_RETRIES,
                        exc,
                    )
                    if attempt < BLE_SESSION_RETRIES:
                        await asyncio.sleep(2.0)
                finally:
                    await self.ble.disconnect()

            raise RuntimeError(
                f"BLE poll failed after {BLE_SESSION_RETRIES} session attempts"
            ) from last_error

    async def command_session(self, coro):
        async with self.ble_op_lock:
            last_error: Optional[Exception] = None
            for attempt in range(1, BLE_SESSION_RETRIES + 1):
                try:
                    await self.ble.connect()
                    result = await coro()
                    regs = await self.ble.read_all()
                    state = self.decode_state(regs)
                    self.publish_state(state)
                    self.publish_availability("online")
                    return result
                except Exception as exc:
                    last_error = exc
                    _LOG.warning(
                        "BLE command session attempt %d/%d failed: %s",
                        attempt,
                        BLE_SESSION_RETRIES,
                        exc,
                    )
                    if attempt < BLE_SESSION_RETRIES:
                        await asyncio.sleep(2.0)
                finally:
                    await self.ble.disconnect()

            raise RuntimeError(
                f"BLE command session failed after {BLE_SESSION_RETRIES} attempts"
            ) from last_error

    def stop(self) -> None:
        self.stop_event.set()

    def device_payload(self) -> dict:
        return {
            "identifiers": [self.device_id()],
            "name": "BL917 Solar Controller",
            "manufacturer": DEVICE_MANUFACTURER,
            "model": DEVICE_MODEL,
            "connections": [["mac", BLE_ADDRESS]],
        }

    def device_id(self) -> str:
        return f"bl917_{BLE_ADDRESS.replace(':', '').lower()}"

    def topic(self, suffix: str) -> str:
        return f"{MQTT_BASE_TOPIC}/{suffix}"

    def discovery_topic(self, component: str, object_id: str) -> str:
        return f"{MQTT_DISCOVERY_PREFIX}/{component}/{object_id}/config"

    def publish(self, topic: str, payload, retain: bool = False) -> None:
        if isinstance(payload, (dict, list)):
            payload = json.dumps(payload, separators=(",", ":"))
        self.mqtt.publish(topic, payload=payload, qos=1, retain=retain)

    def publish_availability(self, status: str) -> None:
        self.publish(self.topic("availability"), status, retain=True)

    def publish_state(self, state: dict) -> None:
        self.publish(self.topic("state"), state, retain=True)

    async def publish_discovery(self) -> None:
        dev = self.device_payload()
        did = self.device_id()
        avail = self.topic("availability")
        state = self.topic("state")

        entities = {
            "sensor": [
                {
                    "object_id": f"{did}_battery_voltage",
                    "payload": {
                        "name": "BL917 Battery Voltage",
                        "unique_id": f"{did}_battery_voltage",
                        "state_topic": state,
                        "availability_topic": avail,
                        "payload_available": "online",
                        "payload_not_available": "offline",
                        "value_template": "{{ value_json.battery_voltage }}",
                        "unit_of_measurement": "V",
                        "device_class": "voltage",
                        "state_class": "measurement",
                        "device": dev,
                    },
                },
                {
                    "object_id": f"{did}_battery_type",
                    "payload": {
                        "name": "BL917 Battery Type",
                        "unique_id": f"{did}_battery_type",
                        "state_topic": state,
                        "availability_topic": avail,
                        "payload_available": "online",
                        "payload_not_available": "offline",
                        "value_template": "{{ value_json.battery_type }}",
                        "device": dev,
                    },
                },
                {
                    "object_id": f"{did}_charge_current",
                    "payload": {
                        "name": "BL917 Charge Current",
                        "unique_id": f"{did}_charge_current",
                        "state_topic": state,
                        "availability_topic": avail,
                        "payload_available": "online",
                        "payload_not_available": "offline",
                        "value_template": "{{ value_json.charge_current }}",
                        "unit_of_measurement": "A",
                        "device_class": "current",
                        "state_class": "measurement",
                        "device": dev,
                    },
                },
                {
                    "object_id": f"{did}_load_current",
                    "payload": {
                        "name": "BL917 Load Current",
                        "unique_id": f"{did}_load_current",
                        "state_topic": state,
                        "availability_topic": avail,
                        "payload_available": "online",
                        "payload_not_available": "offline",
                        "value_template": "{{ value_json.load_current }}",
                        "unit_of_measurement": "A",
                        "device_class": "current",
                        "state_class": "measurement",
                        "device": dev,
                    },
                },
                {
                    "object_id": f"{did}_accumulated_charge",
                    "payload": {
                        "name": "BL917 Accumulated Charge",
                        "unique_id": f"{did}_accumulated_charge",
                        "state_topic": state,
                        "availability_topic": avail,
                        "payload_available": "online",
                        "payload_not_available": "offline",
                        "value_template": "{{ value_json.accumulated_charge }}",
                        "unit_of_measurement": "Ah",
                        "state_class": "total_increasing",
                        "device": dev,
                    },
                },
                {
                    "object_id": f"{did}_controller_temperature",
                    "payload": {
                        "name": "BL917 Controller Temperature",
                        "unique_id": f"{did}_controller_temperature",
                        "state_topic": state,
                        "availability_topic": avail,
                        "payload_available": "online",
                        "payload_not_available": "offline",
                        "value_template": "{{ value_json.controller_temperature }}",
                        "unit_of_measurement": "°C",
                        "device_class": "temperature",
                        "state_class": "measurement",
                        "device": dev,
                    },
                },
            ],
            "switch": [
                {
                    "object_id": f"{did}_load_output",
                    "payload": {
                        "name": "BL917 Load Output",
                        "unique_id": f"{did}_load_output",
                        "state_topic": state,
                        "command_topic": self.topic("cmd/load_output/set"),
                        "availability_topic": avail,
                        "payload_available": "online",
                        "payload_not_available": "offline",
                        "value_template": "{{ 'ON' if value_json.load_output else 'OFF' }}",
                        "payload_on": "ON",
                        "payload_off": "OFF",
                        "device": dev,
                    },
                },
            ],
            "number": [
                {
                    "object_id": f"{did}_cutoff_voltage",
                    "payload": {
                        "name": "BL917 Cutoff Voltage",
                        "unique_id": f"{did}_cutoff_voltage",
                        "state_topic": state,
                        "command_topic": self.topic("cmd/cutoff_voltage/set"),
                        "availability_topic": avail,
                        "payload_available": "online",
                        "payload_not_available": "offline",
                        "value_template": "{{ value_json.cutoff_voltage }}",
                        "unit_of_measurement": "V",
                        "min": 9.0,
                        "max": 15.0,
                        "step": 0.1,
                        "mode": "box",
                        "device": dev,
                    },
                },
                {
                    "object_id": f"{did}_restore_voltage",
                    "payload": {
                        "name": "BL917 Restore Voltage",
                        "unique_id": f"{did}_restore_voltage",
                        "state_topic": state,
                        "command_topic": self.topic("cmd/restore_voltage/set"),
                        "availability_topic": avail,
                        "payload_available": "online",
                        "payload_not_available": "offline",
                        "value_template": "{{ value_json.restore_voltage }}",
                        "unit_of_measurement": "V",
                        "min": 9.0,
                        "max": 16.0,
                        "step": 0.1,
                        "mode": "box",
                        "device": dev,
                    },
                },
            ],
            "select": [
                {
                    "object_id": f"{did}_output_mode",
                    "payload": {
                        "name": "BL917 Output Mode",
                        "unique_id": f"{did}_output_mode",
                        "state_topic": state,
                        "command_topic": self.topic("cmd/output_mode/set"),
                        "availability_topic": avail,
                        "payload_available": "online",
                        "payload_not_available": "offline",
                        "value_template": "{{ value_json.output_mode }}",
                        "options": list(OUTPUT_MODE_MAP_REVERSE.keys()),
                        "device": dev,
                    },
                },
            ],
        }

        for component, items in entities.items():
            for item in items:
                self.publish(
                    self.discovery_topic(component, item["object_id"]),
                    item["payload"],
                    retain=True,
                )

    def decode_state(self, regs: Dict[int, int]) -> dict:
        state = {
            "raw_registers": {f"0x{k:04X}": v for k, v in sorted(regs.items())},
            "last_update": int(time.time()),
        }

        for reg, value in regs.items():
            spec = MAP_TELEMETRY.get(reg)
            if not spec:
                continue
            key, scale = spec
            state[key] = round(value * scale, 3) if scale != 1.0 else value

        raw_load = regs.get(REG_LOAD_OUTPUT)
        if raw_load is not None:
            state["load_output"] = bool(raw_load)

        raw_mode = regs.get(REG_OUTPUT_MODE)
        if raw_mode is not None:
            state["output_mode"] = OUTPUT_MODE_MAP.get(raw_mode, f"unknown_{raw_mode}")

        raw_battery_type = regs.get(0x1001, regs.get(0x0001))
        if raw_battery_type is not None:
            state["battery_type"] = BATTERY_TYPE_MAP.get(raw_battery_type, f"unknown_{raw_battery_type}")

        return state

    def _on_mqtt_connect(self, client, _userdata, _flags, reason_code, _properties) -> None:
        _LOG.info("MQTT connected: rc=%s", reason_code)
        client.subscribe(self.topic("cmd/+/set"), qos=1)

    def _on_mqtt_disconnect(self, _client, _userdata, _flags, reason_code, _properties) -> None:
        _LOG.warning("MQTT disconnected: rc=%s", reason_code)

    def _on_mqtt_message(self, _client, _userdata, msg) -> None:
        if not self.loop:
            return
        topic = msg.topic
        payload = msg.payload.decode("utf-8", errors="replace").strip()
        self.loop.call_soon_threadsafe(
            lambda: asyncio.create_task(self.handle_command(topic, payload))
        )

    async def handle_command(self, topic: str, payload: str) -> None:
        _LOG.info("MQTT command %s -> %s", topic, payload)

        try:
            if topic.endswith("/cmd/load_output/set"):
                async def _cmd():
                    if payload.upper() == "ON":
                        await self.ble.write_register(REG_LOAD_OUTPUT, 1)
                    elif payload.upper() == "OFF":
                        await self.ble.write_register(REG_LOAD_OUTPUT, 0)
                    else:
                        raise ValueError("load_output payload must be ON or OFF")
                await self.command_session(_cmd)

            elif topic.endswith("/cmd/cutoff_voltage/set"):
                volts = float(payload)
                raw = int(round(volts * 10))

                async def _cmd():
                    await self.ble.write_register(REG_CUTOFF_VOLTAGE, raw)
                await self.command_session(_cmd)

            elif topic.endswith("/cmd/restore_voltage/set"):
                volts = float(payload)
                raw = int(round(volts * 10))

                async def _cmd():
                    await self.ble.write_register(REG_RESTORE_VOLTAGE, raw)
                await self.command_session(_cmd)

            elif topic.endswith("/cmd/output_mode/set"):
                mode_raw = OUTPUT_MODE_MAP_REVERSE.get(payload)
                if mode_raw is None:
                    raise ValueError(f"invalid output_mode {payload!r}")

                async def _cmd():
                    await self.ble.write_register(REG_OUTPUT_MODE, mode_raw)
                await self.command_session(_cmd)

            else:
                _LOG.warning("Unhandled topic: %s", topic)

        except Exception:
            _LOG.exception("Failed to handle command topic=%s payload=%s", topic, payload)
            self.publish_availability("offline")


# =========================
# Entrypoint
# =========================

async def async_main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    ble = BL917Client(BLE_ADDRESS)
    bridge = BL917MqttBridge(ble)

    loop = asyncio.get_running_loop()

    def _request_stop():
        bridge.stop()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _request_stop)
        except NotImplementedError:
            pass

    await bridge.start()


if __name__ == "__main__":
    asyncio.run(async_main())
