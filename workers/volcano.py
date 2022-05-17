import logger
import asyncio
import threading
import sys
from typing import Final, TypedDict

from mqtt import MqttMessage, MqttConfigMessage
from workers.base import BaseWorker, retry

sys.path.insert(0, "/home/robbie/Documents/Programming/Personal/volcano-bt/")

_LOGGER = logger.get(__name__)

REQUIREMENTS = [
    "git+https://github.com/hookedonunix/volcano-bt.git#egg=volcanobt",
    "werkzeug",
]

STATE_HEAT = "heat"
STATE_OFF = "off"

FAN_ON = "on"
FAN_OFF = "off"

FAN_MODE_AUTO = "auto"

SENSOR_CLIMATE = "climate"
SENSOR_CURRENT_TEMPERATURE = "current_temperature"
SENSOR_TARGET_TEMPERATURE = "target_temperature"
SENSOR_SERIAL_NUMBER = "serial_number"
SENSOR_FIRMWARE_VERSION = "firmware_version"
SENSOR_OPERATION_HOURS = "operation_hours"

SWITCH_HEATER = "heater"
SWITCH_PUMP = "pump"

SWITCH_VIBRATION = "vibration"
SWITCH_DISPLAY_ON_COOLING = "display_on_cooling"

LIGHT_LED_DISPLAY = "led_display"

SELECT_TEMPERATURE_UNIT = "temperature_unit"

TEMP_CELSIUS: Final = "°C"
TEMP_FAHRENHEIT: Final = "°F"

monitoredAttrs = [
    SENSOR_CURRENT_TEMPERATURE,
    SENSOR_TARGET_TEMPERATURE,
    SENSOR_SERIAL_NUMBER,
    SENSOR_FIRMWARE_VERSION,
    SENSOR_OPERATION_HOURS,
    SWITCH_HEATER,
    SWITCH_PUMP,
    SWITCH_VIBRATION,
    SWITCH_DISPLAY_ON_COOLING,
]

def celsius_to_fahrenheit(temp: int):
    return (temp * 1.8) + 32

class MqttDiscoveryConfig(TypedDict):
    mac: str
    name: str
    availability_topic: str

class VolcanoWorker(BaseWorker):
    def _setup(self):
        from volcanobt.volcano import Volcano

        _LOGGER.info("Adding %d %s devices", len(self.devices), repr(self))
        for name, obj in self.devices.items():
            if isinstance(obj, str):
                self.devices[name] = {"mac": obj, "volcano": Volcano(obj)}
            elif isinstance(obj, dict):
                self.devices[name] = {
                    "mac": obj["mac"],
                    "volcano": Volcano(obj["mac"]),
                }
            else:
                raise TypeError("Unsupported configuration format")
            _LOGGER.debug(
                "Adding %s device '%s' (%s)",
                repr(self),
                name,
                self.devices[name]["mac"]
            )

    def config(self, availability_topic):
        ret = []
        for name, data in self.devices.items():
            ret += self.config_device(name, data, availability_topic)
        return ret

    def config_device(self, name, data, availability_topic):
        config = {
            "mac": data["mac"],
            "name": name,
            "availability_topic": availability_topic,
        }

        return [
            self.generate_climate_discovery(config),

            self.generate_temp_sensor_discovery(config, SENSOR_TARGET_TEMPERATURE, "temperature",),
            self.generate_temp_sensor_discovery(config, SENSOR_CURRENT_TEMPERATURE, "temperature"),

            self.generate_sensor_discovery(config, SENSOR_OPERATION_HOURS),
            self.generate_sensor_discovery(config, SENSOR_SERIAL_NUMBER),
            self.generate_sensor_discovery(config, SENSOR_FIRMWARE_VERSION),

            self.generate_switch_discovery(config, SWITCH_HEATER, None, "mdi:radiator"),
            self.generate_switch_discovery(config, SWITCH_PUMP, None, "mdi:pump"),
            self.generate_switch_discovery(config, SWITCH_VIBRATION, None, "mdi:volume-vibrate"),
            self.generate_switch_discovery(config, SWITCH_DISPLAY_ON_COOLING, None, "mdi:television-ambient-light"),

            self.generate_light_discovery(config, LIGHT_LED_DISPLAY),
            self.generate_select_discovery(config, SELECT_TEMPERATURE_UNIT, [TEMP_CELSIUS, TEMP_FAHRENHEIT], "mdi:home-thermometer"),
        ]

    def generate_device_discovery(self, config: MqttDiscoveryConfig, volcano = None) -> dict:
        mac = config["mac"]
        name = config["name"]

        device = {
            "identifiers": [mac, self.format_discovery_id(mac, name)],
            "manufacturer": "Storz & Bickel",
            "model": "Volcano Hybrid",
            "name": "Volcano",
        }

        if volcano is not None:
            device["sw_version"] = volcano.firmware_version
        
        return device

    def generate_climate_discovery(self, config: MqttDiscoveryConfig, volcano = None) -> MqttConfigMessage:
        mac = config["mac"]
        name = config["name"]
        availability_topic = config["availability_topic"]
        temp_unit = TEMP_CELSIUS

        if volcano is not None:
            temp_unit = volcano.temperature_unit

        payload = {
            "unique_id": self.format_discovery_id(mac, name, SENSOR_CLIMATE),
            "name": self.format_discovery_name(name, SENSOR_CLIMATE),
            "qos": 1,
            "temp_step": 1,
            "min_temp": 40.0 if temp_unit == TEMP_CELSIUS else celsius_to_fahrenheit(40.0),
            "max_temp": 230.0 if temp_unit == TEMP_CELSIUS else celsius_to_fahrenheit(230.0),
            "temperature_unit": "C" if temp_unit == TEMP_CELSIUS else "F",
            "availability_topic": availability_topic,
            "temperature_state_topic": self.format_prefixed_topic(
                name, SENSOR_TARGET_TEMPERATURE
            ),
            "temperature_command_topic": self.format_prefixed_topic(
                name, SENSOR_TARGET_TEMPERATURE, "set"
            ),
            "current_temperature_topic": self.format_prefixed_topic(
                name, SENSOR_CURRENT_TEMPERATURE
            ),
            "mode_state_topic": self.format_prefixed_topic(name, "mode"),
            "mode_command_topic": self.format_prefixed_topic(name, "mode", "set"),
            "fan_mode_state_topic": self.format_prefixed_topic(name, "fan"),
            "fan_mode_command_topic": self.format_prefixed_topic(name, "fan", "set"),
            "json_attributes_topic": self.format_prefixed_topic(
                name, "json_attributes"
            ),
            "modes": [STATE_HEAT, STATE_OFF],
            "fan_modes": [FAN_MODE_AUTO],
            "device": self.generate_device_discovery(config, volcano),
        }

        return MqttConfigMessage(
            MqttConfigMessage.CLIMATE,
            self.format_discovery_topic(mac, name, SENSOR_CLIMATE),
            payload=payload,
        )

    def generate_sensor_discovery(self, config: MqttDiscoveryConfig, sensor: str, volcano = None) -> MqttConfigMessage:
        mac = config["mac"]
        name = config["name"]
        availability_topic = config["availability_topic"]

        payload = {
            "unique_id": self.format_discovery_id(mac, name, sensor),
            "name": self.format_discovery_name(name, sensor),
            "state_topic": self.format_prefixed_topic(name, sensor),
            "availability_topic": availability_topic,
            "device": self.generate_device_discovery(config, volcano),
        }

        return MqttConfigMessage(
            MqttConfigMessage.SENSOR,
            self.format_discovery_topic(mac, name, sensor),
            payload=payload,
        )

    def generate_temp_sensor_discovery(self, config: MqttDiscoveryConfig, sensor: str, device_class: str = None, volcano = None) -> MqttConfigMessage:
        mac = config["mac"]
        name = config["name"]
        availability_topic = config["availability_topic"]

        temp_unit = TEMP_CELSIUS

        if volcano is not None:
            temp_unit = volcano.temperature_unit

        payload = {
            "unique_id": self.format_discovery_id(mac, name, sensor),
            "name": self.format_discovery_name(name, sensor),
            "state_topic": self.format_prefixed_topic(name, sensor),
            "availability_topic": availability_topic,
            "payload_on": "true",
            "payload_off": "false",
            "device": self.generate_device_discovery(config, volcano),
            "unit_of_measurement": temp_unit,
        }

        if device_class is not None:
            payload["device_class"] = device_class

        return MqttConfigMessage(
            MqttConfigMessage.SENSOR,
            self.format_discovery_topic(mac, name, sensor),
            payload=payload,
        )

    def generate_switch_discovery(self, config: MqttDiscoveryConfig, switch: str, device_class: str, icon: str = None, volcano = None) -> MqttConfigMessage:
        mac = config["mac"]
        name = config["name"]
        availability_topic = config["availability_topic"]

        payload = {
            "unique_id": self.format_discovery_id(mac, name, switch),
            "name": self.format_discovery_name(name, switch),
            "state_topic": self.format_prefixed_topic(name, switch),
            "availability_topic": availability_topic,
            "command_topic": self.format_prefixed_topic(name, switch, "set"),
            "device": self.generate_device_discovery(config, volcano),
        }

        if device_class is not None:
            payload["device_class"] = device_class

        if icon is not None:
            payload["icon"] = icon

        return MqttConfigMessage(
            MqttConfigMessage.SWITCH,
            self.format_discovery_topic(mac, name, switch),
            payload=payload,
        )

    def generate_light_discovery(self, config: MqttDiscoveryConfig, light: str, icon: str = None, volcano = None) -> MqttConfigMessage:
        mac = config["mac"]
        name = config["name"]
        availability_topic = config["availability_topic"]

        payload = {
            "unique_id": self.format_discovery_id(mac, name, light),
            "name": self.format_discovery_name(name, light),
            "brightness": True,
            "brightness_state_topic": self.format_prefixed_topic(name, light, "brightness"),
            "brightness_command_topic": self.format_prefixed_topic(name, light, "brightness", "set"),
            "brightness_scale": 100,
            "availability_topic": availability_topic,
            "state_topic": self.format_prefixed_topic(name, light),
            "command_topic": self.format_prefixed_topic(name, light, "set"),
            "device": self.generate_device_discovery(config),
        }

        return MqttConfigMessage(
            "light",
            self.format_discovery_topic(mac, name, light),
            payload=payload
        )

    def generate_select_discovery(self, config: MqttDiscoveryConfig, select: str, options: list[str], icon: str = None, volcano = None) -> MqttConfigMessage:
        mac = config["mac"]
        name = config["name"]
        availability_topic = config["availability_topic"]

        payload = {
            "unique_id": self.format_discovery_id(mac, name, select),
            "name": self.format_discovery_name(name, select),
            "state_topic": self.format_prefixed_topic(name, select),
            "availability_topic": availability_topic,
            "command_topic": self.format_prefixed_topic(name, select, "set"),
            "device": self.generate_device_discovery(config),
            "options": options
        }

        if (icon):
            payload["icon"] = icon

        return MqttConfigMessage(
            "select",
            self.format_discovery_topic(mac, name, select),
            payload=payload
        )

    def run(self, mqtt, stop_event):
        self._stop_event: threading.Event = stop_event
        self._loop = asyncio.new_event_loop()
        self._loop.run_until_complete(self.daemon(mqtt))
        self._loop.stop()

    async def daemon(self, mqtt):
        from volcanobt.volcano import Volcano

        asyncio.set_event_loop(self._loop)

        volcano: Volcano = self.devices["volcano"]["volcano"]

        name = "volcano"
        mac = self.devices["volcano"]["mac"]

        config = {
            "mac": self.devices["volcano"]["mac"],
            "name": name,
            "availability_topic": "lwt_topic",
        }

        def temperature_changed(temperature: int):
            _LOGGER.info(f"Temperature changed {temperature}{TEMP_CELSIUS}")
            mqtt.publish([
                MqttMessage(topic=self.format_topic("volcano", SENSOR_CURRENT_TEMPERATURE), payload=volcano.temperature)
            ])

        volcano.on_temperature_changed(temperature_changed)

        def target_temperature_changed(temperature: int):
            _LOGGER.info(f"Target temperature changed {temperature}")
            mqtt.publish([
                MqttMessage(topic=self.format_topic("volcano", SENSOR_TARGET_TEMPERATURE), payload=temperature)
            ])

        volcano.on_target_temperature_changed(target_temperature_changed)

        def heater_changed(state: bool):
            _LOGGER.info(f"Heater changed to {state}")
            mqtt.publish([
                MqttMessage(topic=self.format_topic("volcano", "mode"), payload="heat" if state else "off"),
                MqttMessage(topic=self.format_topic("volcano", SWITCH_HEATER), payload=self.true_false_to_ha_on_off(state)),
            ])

        volcano.on_heater_changed(heater_changed)

        def pump_changed(state: bool):
            _LOGGER.info(f"Pump changed to {state}")
            mqtt.publish([
                MqttMessage(topic=self.format_topic("volcano", "fan"), payload=FAN_MODE_AUTO),
                MqttMessage(topic=self.format_topic("volcano", SWITCH_PUMP), payload=self.true_false_to_ha_on_off(state)),
            ])

        volcano.on_pump_changed(pump_changed)

        def temperature_unit_changed(unit):
            _LOGGER.info(f"Temperature unit changed to '{unit}'")
            climate_msg = self.generate_climate_discovery(config, volcano)
            climate_msg.topic = "{}/{}".format("homeassistant", climate_msg.topic)

            curr_temp_msg = self.generate_temp_sensor_discovery(config, SENSOR_CURRENT_TEMPERATURE, "temperature", volcano)
            curr_temp_msg.topic = "{}/{}".format("homeassistant", curr_temp_msg.topic)

            target_temp_msg = self.generate_temp_sensor_discovery(config, SENSOR_TARGET_TEMPERATURE, "temperature", volcano)
            target_temp_msg.topic = "{}/{}".format("homeassistant", target_temp_msg.topic)

            mqtt.publish([climate_msg, curr_temp_msg, target_temp_msg])

            mqtt.publish([
                MqttMessage(topic=self.format_topic("volcano", SELECT_TEMPERATURE_UNIT), payload=unit),
                MqttMessage(topic=self.format_topic("volcano", SENSOR_CURRENT_TEMPERATURE), payload=volcano.temperature),
                MqttMessage(topic=self.format_topic("volcano", SENSOR_TARGET_TEMPERATURE), payload=volcano.target_temperature),
            ])

        volcano.on_temperature_unit_changed(temperature_unit_changed)

        def display_on_cooling_changed(state: bool):
            _LOGGER.info(f"Display on cooling changed to {state}")
            mqtt.publish([
                MqttMessage(topic=self.format_topic("volcano", "display_on_cooling"), payload=self.true_false_to_ha_on_off(state)),
            ])

        volcano.on_display_on_cooling_changed(display_on_cooling_changed)

        _LOGGER.info(f"Connecting to Volcano [{mac}]")
        await volcano.connect()
        
        _LOGGER.info("Reading attributes from BLE GATT`")
        await volcano.read_attributes()

        _LOGGER.info(f"Temperature            {volcano.temperature}{volcano.temperature_unit}")
        _LOGGER.info(f"Target temperature     {volcano.target_temperature}{volcano.temperature_unit}")
        _LOGGER.info(f"Serial number          {volcano.serial_number}")
        _LOGGER.info(f"Firmware version       {volcano.firmware_version}")
        _LOGGER.info(f"BLE firmware version   {volcano.ble_firmware_version}")
        _LOGGER.info(f"Auto off time          {volcano.auto_off_time}s")
        _LOGGER.info(f"Shut off time          {volcano.shut_off_time}m")
        _LOGGER.info(f"Vibration enabled      {volcano.vibration_enabled}")
        _LOGGER.info(f"Auto off enabled       {volcano.auto_off_enabled}")
        _LOGGER.info(f"LED brightness         {volcano.led_brightness}%")
        _LOGGER.info(f"Operation hours        {volcano.operation_hours}h")
        _LOGGER.info(f"Temperature unit       {volcano.temperature_unit}")
        _LOGGER.info(f"Display on cooling     {volcano.display_on_cooling}")


        msg = self.generate_climate_discovery(config, volcano)
        msg.topic = "{}/{}".format("homeassistant", msg.topic)
        msg.retain = True
        mqtt.publish([msg])

        async def status_update():
            while not self._stop_event.is_set():
                _LOGGER.info("Updating volcano attributes")
                mqtt.publish([
                    MqttMessage(topic=self.format_topic("volcano", SENSOR_CURRENT_TEMPERATURE), payload=volcano.temperature),
                    MqttMessage(topic=self.format_topic("volcano", SENSOR_TARGET_TEMPERATURE), payload=volcano.target_temperature),
                    MqttMessage(topic=self.format_topic("volcano", "fan"), payload=FAN_MODE_AUTO),
                    MqttMessage(topic=self.format_topic("volcano", "mode"), payload="heat" if volcano.heater_on else "off"),
                    MqttMessage(topic=self.format_topic("volcano", SENSOR_SERIAL_NUMBER), payload=volcano.serial_number),
                    MqttMessage(topic=self.format_topic("volcano", SENSOR_FIRMWARE_VERSION), payload=volcano.firmware_version),
                    MqttMessage(topic=self.format_topic("volcano", SWITCH_HEATER), payload=self.true_false_to_ha_on_off(volcano.heater_on)),
                    MqttMessage(topic=self.format_topic("volcano", SWITCH_PUMP), payload=self.true_false_to_ha_on_off(volcano.pump_on)),
                    MqttMessage(topic=self.format_topic("volcano", SWITCH_VIBRATION), payload=self.true_false_to_ha_on_off(volcano.vibration_enabled)),
                    MqttMessage(topic=self.format_topic("volcano", SWITCH_DISPLAY_ON_COOLING), payload=self.true_false_to_ha_on_off(volcano.display_on_cooling)),
                    MqttMessage(topic=self.format_topic("volcano", LIGHT_LED_DISPLAY), payload=self.true_false_to_ha_on_off(True)),
                    MqttMessage(topic=self.format_topic("volcano", LIGHT_LED_DISPLAY, "brightness"), payload=volcano.led_brightness),
                    MqttMessage(topic=self.format_topic("volcano", SELECT_TEMPERATURE_UNIT), payload=volcano.temperature_unit),
                ])
                await asyncio.sleep(60.0)

        task = asyncio.create_task(status_update())

        async def wait_for_stop():
            while not self._stop_event.is_set():
                self._stop_event.wait(0.5)
                await asyncio.sleep(0.5)
            task.cancel()

        try:
            await asyncio.gather(
                task,
                asyncio.create_task(wait_for_stop()),
            )
        except asyncio.CancelledError:
            pass

        await volcano.disconnect()

    def on_command(self, topic, value):
        from werkzeug.routing import Map, Rule

        asyncio.set_event_loop(self._loop)

        topic_without_prefix = topic.replace("{}/".format(self.topic_prefix), "", 1)

        route_map = Map([
            Rule(f"/<volcano>/{SENSOR_TARGET_TEMPERATURE}/set", endpoint=self._on_volcano_target_temperature.__name__),
            Rule(f"/<volcano>/{SWITCH_HEATER}/set", endpoint=self._on_volcano_heater.__name__),
            Rule(f"/<volcano>/{SWITCH_PUMP}/set", endpoint=self._on_volcano_pump.__name__),
            Rule("/<volcano>/mode/set", endpoint=self._on_volcano_mode.__name__),
            Rule("/<volcano>/fan/set", endpoint=self._on_volcano_fan.__name__),
            Rule(f"/<volcano>/{SWITCH_VIBRATION}/set", endpoint=self._on_volcano_vibration.__name__),
            Rule(f"/<volcano>/{SWITCH_DISPLAY_ON_COOLING}/set", endpoint=self._on_volcano_display_on_cooling.__name__),
            Rule(f"/<volcano>/{LIGHT_LED_DISPLAY}/brightness/set", endpoint=self._on_volcano_led_brightness.__name__),
            Rule(f"/<volcano>/{SELECT_TEMPERATURE_UNIT}/set", endpoint=self._on_volcano_temperature_unit.__name__),
            Rule("/<path:path>", endpoint=self._on_any.__name__),
        ])

        router = route_map.bind("example.com/volcano/", "/")

        (callable, kwargs) = router.match(topic_without_prefix)

        if "volcano" in kwargs:
            if kwargs["volcano"] in self.devices:
                volcano = self.devices[kwargs["volcano"]]["volcano"]

                if not volcano.is_connected:
                    asyncio.run_coroutine_threadsafe(volcano.connect(), self._loop).result(10.0)

                kwargs["volcano"] = volcano
            else:
                logger.log_exception(_LOGGER, "Ignore command because device %s is unknown", kwargs["volcano"])
                return []

        kwargs["value"] = value.decode("utf-8")

        return getattr(self, callable)(**kwargs)

    def _on_volcano_target_temperature(self, volcano, value: str):
        temperature = round(float(value))
        asyncio.run_coroutine_threadsafe(volcano.set_target_temperature(temperature), self._loop).result(10.0)

        return [MqttMessage(topic=self.format_topic("volcano", SENSOR_TARGET_TEMPERATURE), payload=temperature)]

    def _on_volcano_mode(self, volcano, value: str):
        state = True if value == "heat" else False
        asyncio.run_coroutine_threadsafe(volcano.set_heater(state), self._loop).result(10.0)

        return [MqttMessage(topic=self.format_topic("volcano", "mode"), payload=value)]

    def _on_volcano_fan(self, volcano, value: str):
        state = True if value == "ON" else False
        asyncio.run_coroutine_threadsafe(volcano.set_heater(state), self._loop).result(10.0)

        return [MqttMessage(topic=self.format_topic("volcano", "fan"), payload=FAN_MODE_AUTO)]

    def _on_volcano_heater(self, volcano, value: str):
        state = True if value == "ON" else False
        asyncio.run_coroutine_threadsafe(volcano.set_heater(state), self._loop).result(10.0)

        return [MqttMessage(topic=self.format_topic("volcano", SWITCH_HEATER), payload=value)]

    def _on_volcano_pump(self, volcano, value: str):
        state = True if value == "ON" else False
        asyncio.run_coroutine_threadsafe(volcano.set_pump(state), self._loop).result(10.0)

        return [MqttMessage(topic=self.format_topic("volcano", SWITCH_PUMP), payload=value)]

    def _on_volcano_led_brightness(self, volcano, value: str):
        brightness = round(float(value))
        asyncio.run_coroutine_threadsafe(volcano.set_led_brightness(brightness), self._loop).result(10.0)

        return [MqttMessage(topic=self.format_topic("volcano", LIGHT_LED_DISPLAY, "brightness"), payload=brightness)]
    
    def _on_volcano_temperature_unit(self, volcano, value: str):
        asyncio.run_coroutine_threadsafe(volcano.set_temperature_unit(value), self._loop).result(10.0)

        return [MqttMessage(topic=self.format_topic("volcano", SELECT_TEMPERATURE_UNIT), payload=value)]

    def _on_volcano_vibration(self, volcano, value: str):
        state = True if value == "ON" else False
        asyncio.run_coroutine_threadsafe(volcano.set_vibration_enabled(state), self._loop).result(10.0)

        return [MqttMessage(topic=self.format_topic("volcano", SWITCH_VIBRATION), payload=value)]

    def _on_volcano_display_on_cooling(self, volcano, value: str):
        state = True if value == "ON" else False
        asyncio.run_coroutine_threadsafe(volcano.set_display_on_cooling(state), self._loop).result(10.0)

        return [MqttMessage(topic=self.format_topic("volcano", SWITCH_DISPLAY_ON_COOLING), payload=value)]

    def _on_any(self, path: str, value: str):
        return []
