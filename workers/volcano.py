import logger
import asyncio
import threading
import sys

from mqtt import MqttMessage, MqttConfigMessage
from workers.base import BaseWorker, retry

_LOGGER = logger.get(__name__)

REQUIREMENTS = [
    "git+https://github.com/hookedonunix/volcano-bt.git#egg=volcanobt",
    "werkzeug",
]

sys.path.insert(0, '/home/robbie/Documents/Programming/Personal/volcano-bt')

STATE_HEAT = "heat"
STATE_OFF = "off"

FAN_ON = "on"
FAN_OFF = "off"

FAN_MODE_AUTO = "auto"

HOLD_NONE = "none"
HOLD_BOOST = "boost"
HOLD_COMFORT = "comfort"
HOLD_ECO = "eco"

SENSOR_CLIMATE = "climate"
SENSOR_WINDOW = "window_open"
SENSOR_BATTERY = "low_battery"
SENSOR_LOCKED = "locked"
SENSOR_VALVE = "valve_state"
SENSOR_AWAY_END = "away_end"
SENSOR_TARGET_TEMPERATURE = "target_temperature"
SENSOR_CURRENT_TEMPERATURE = "current_temperature"
SENSOR_PUMP_STATE = "pump"
SENSOR_HEATER_STATE = "heater"

monitoredAttrs = [
    SENSOR_BATTERY,
    SENSOR_VALVE,
    SENSOR_TARGET_TEMPERATURE,
    SENSOR_WINDOW,
    SENSOR_LOCKED,
]


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
        ret = []
        mac = data["mac"]
        device = {
            "identifiers": [mac, self.format_discovery_id(mac, name)],
            "manufacturer": "Storz & Bickel",
            "model": "Volcano Hybrid",
            "name": "Volcano",
            "sw_version": "0"
        }

        _LOGGER.info(device)

        payload = {
            "unique_id": self.format_discovery_id(mac, name, SENSOR_CLIMATE),
            "name": self.format_discovery_name(name, SENSOR_CLIMATE),
            "qos": 1,
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
            "min_temp": 40.0,
            "max_temp": 230.0,
            "temp_step": 1,
            "modes": [STATE_HEAT, STATE_OFF],
            "fan_modes": [FAN_MODE_AUTO],
            "device": device,
        }

        ret.append(
            MqttConfigMessage(
                MqttConfigMessage.CLIMATE,
                self.format_discovery_topic(mac, name, SENSOR_CLIMATE),
                payload=payload,
            )
        )

        payload = {
            "unique_id": self.format_discovery_id(mac, name, SENSOR_TARGET_TEMPERATURE),
            "name": self.format_discovery_name(name, SENSOR_TARGET_TEMPERATURE),
            "state_topic": self.format_prefixed_topic(name, SENSOR_TARGET_TEMPERATURE),
            "availability_topic": availability_topic,
            "payload_on": "true",
            "payload_off": "false",
            "device": device,
            "unit_of_measurement": "°C"
        }

        ret.append(
            MqttConfigMessage(
                MqttConfigMessage.SENSOR,
                self.format_discovery_topic(mac, name, SENSOR_TARGET_TEMPERATURE),
                payload=payload
            )
        )

        payload = {
            "unique_id": self.format_discovery_id(mac, name, SENSOR_CURRENT_TEMPERATURE),
            "name": self.format_discovery_name(name, SENSOR_CURRENT_TEMPERATURE),
            "state_topic": self.format_prefixed_topic(name, SENSOR_CURRENT_TEMPERATURE),
            "availability_topic": availability_topic,
            "payload_on": "true",
            "payload_off": "false",
            "device": device,
            "unit_of_measurement": "°C"
        }

        ret.append(
            MqttConfigMessage(
                MqttConfigMessage.SENSOR,
                self.format_discovery_topic(mac, name, SENSOR_CURRENT_TEMPERATURE),
                payload=payload
            )
        )

        payload = {
            "unique_id": self.format_discovery_id(mac, name, SENSOR_PUMP_STATE),
            "name": self.format_discovery_name(name, SENSOR_PUMP_STATE),
            "state_topic": self.format_prefixed_topic(name, SENSOR_PUMP_STATE),
            "availability_topic": availability_topic,
            "command_topic": self.format_prefixed_topic(name, SENSOR_PUMP_STATE, 'set'),
            "device": device,
        }

        ret.append(
            MqttConfigMessage(
                MqttConfigMessage.SWITCH,
                self.format_discovery_topic(mac, name, SENSOR_PUMP_STATE),
                payload=payload
            )
        )

        payload = {
            "unique_id": self.format_discovery_id(mac, name, SENSOR_HEATER_STATE),
            "name": self.format_discovery_name(name, SENSOR_HEATER_STATE),
            "state_topic": self.format_prefixed_topic(name, SENSOR_HEATER_STATE),
            "availability_topic": availability_topic,
            "command_topic": self.format_prefixed_topic(name, SENSOR_HEATER_STATE, 'set'),
            "device": device,
        }

        ret.append(
            MqttConfigMessage(
                MqttConfigMessage.SWITCH,
                self.format_discovery_topic(mac, name, SENSOR_HEATER_STATE),
                payload=payload
            )
        )

        payload = {
            "unique_id": self.format_discovery_id(mac, name, 'temperature_unit'),
            "name": self.format_discovery_name(name, 'temperature_unit'),
            "state_topic": self.format_prefixed_topic(name, 'temperature_unit'),
            "availability_topic": availability_topic,
            "command_topic": self.format_prefixed_topic(name, 'temperature_unit', 'set'),
            "device": device,
            "options": ["°C", "°F"]
        }

        ret.append(
            MqttConfigMessage(
                'select',
                self.format_discovery_topic(mac, name, 'temperature_unit'),
                payload=payload
            )
        )

        payload = {
            "unique_id": self.format_discovery_id(mac, name, 'serial_number'),
            "name": self.format_discovery_name(name, 'serial_number'),
            "state_topic": self.format_prefixed_topic(name, 'serial_number'),
            "availability_topic": availability_topic,
            "device": device,
        }

        ret.append(
            MqttConfigMessage(
                MqttConfigMessage.SENSOR,
                self.format_discovery_topic(mac, name, 'serial_number'),
                payload=payload
            )
        )

        payload = {
            "unique_id": self.format_discovery_id(mac, name, 'firmware_version'),
            "name": self.format_discovery_name(name, 'firmware_version'),
            "state_topic": self.format_prefixed_topic(name, 'firmware_version'),
            "availability_topic": availability_topic,
            "device": device,
        }

        ret.append(
            MqttConfigMessage(
                MqttConfigMessage.SENSOR,
                self.format_discovery_topic(mac, name, 'firmware_version'),
                payload=payload
            )
        )

        payload = {
            "unique_id": self.format_discovery_id(mac, name, 'led_display'),
            "name": self.format_discovery_name(name, 'led_display'),
            "brightness": True,
            "brightness_state_topic": self.format_prefixed_topic(name, 'led_display', 'brightness'),
            "brightness_command_topic": self.format_prefixed_topic(name, 'led_display', 'brightness', 'set'),
            "brightness_scale": 100,
            "availability_topic": availability_topic,
            "state_topic": self.format_prefixed_topic(name, 'led_display'),
            "command_topic": self.format_prefixed_topic(name, 'led_display', 'set'),
            "device": device,
        }

        ret.append(
            MqttConfigMessage(
                'light',
                self.format_discovery_topic(mac, name, 'led_display'),
                payload=payload
            )
        )

        _LOGGER.info(ret)

        return ret

    def run(self, mqtt, stop_event):
        self._stop_event: threading.Event = stop_event
        self._loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self._loop)

        _LOGGER.info("Before start daemon")
        self._loop.run_until_complete(self.daemon(mqtt))
        _LOGGER.info("After start daemon")

        self._loop.stop()

    async def daemon(self, mqtt):
        from volcanobt.volcano import Volcano

        asyncio.set_event_loop(self._loop)

        _LOGGER.info("Before get volcano")
        volcano: Volcano = self.devices['volcano']['volcano']

        def temperature_changed(temperature):
            _LOGGER.info(f"Temperature changed {temperature}")
            mqtt.publish([
                MqttMessage(topic=self.format_topic('volcano', SENSOR_CURRENT_TEMPERATURE), payload=temperature)
            ])

        volcano.on_temperature_changed(temperature_changed)

        def target_temperature_changed(temperature):
            _LOGGER.info(f"Target temperature changed {temperature}")
            mqtt.publish([
                MqttMessage(topic=self.format_topic('volcano', SENSOR_TARGET_TEMPERATURE), payload=temperature)
            ])

        volcano.on_target_temperature_changed(target_temperature_changed)

        def heater_changed(state):
            _LOGGER.info(f"Heater changed to {state}")
            mqtt.publish([
                MqttMessage(topic=self.format_topic('volcano', "mode"), payload="heat" if state else "off"),
                MqttMessage(topic=self.format_topic('volcano', SENSOR_HEATER_STATE), payload=self.true_false_to_ha_on_off(state)),
            ])

        volcano.on_heater_changed(heater_changed)

        def pump_changed(state):
            _LOGGER.info(f"Pump changed to {state}")
            mqtt.publish([
                MqttMessage(topic=self.format_topic('volcano', "fan"), payload=self.true_false_to_ha_on_off(state)),
                MqttMessage(topic=self.format_topic('volcano', SENSOR_PUMP_STATE), payload=self.true_false_to_ha_on_off(state)),
            ])

        volcano.on_pump_changed(pump_changed)

        _LOGGER.info("Before connect")
        await volcano.connect()
        _LOGGER.info("After connect")
        
        _LOGGER.info("Initializing metrics")
        await volcano.initialize_metrics()

        _LOGGER.info(f"Temperature    '{volcano.temperature}'")
        _LOGGER.info(f"Target temperature    '{volcano.target_temperature}'")
        _LOGGER.info(f"Serial number    '{volcano.serial_number}'")
        _LOGGER.info(f"Firmware version    '{volcano.firmware_version}'")
        _LOGGER.info(f"BLE firmware version     '{volcano.ble_firmware_version}'")
        _LOGGER.info(f"Auto off time     '{volcano.auto_off_time}'")
        _LOGGER.info(f"Shut off time     '{volcano.shut_off_time}'")
        _LOGGER.info(f"Vibration enabled     '{volcano.vibration_enabled}'")
        _LOGGER.info(f"LED brightness     '{volcano.led_brightness}'")
        _LOGGER.info(f"Operation hours     '{volcano.operation_hours}'")
        _LOGGER.info(f"Temperature unit    '{volcano.temperature_unit}'")

        await asyncio.sleep(5.0)

        await volcano.set_temperature_unit("°C")

        await asyncio.sleep(3.0)

        await volcano.read_temperature_unit_register()

        _LOGGER.info(volcano.temperature_unit)

        name = 'volcano'
        mac = self.devices['volcano']['mac']

        device = {
            "identifiers": [mac, self.format_discovery_id(mac, 'volcano')],
            "manufacturer": "Storz & Bickel",
            "model": "Volcano Hybrid",
            "name": "Volcano",
            "sw_version": volcano.firmware_version,
        }

        payload = {
            "unique_id": self.format_discovery_id(mac, name, SENSOR_CLIMATE),
            "name": self.format_discovery_name(name, SENSOR_CLIMATE),
            "qos": 1,
            "availability_topic": 'lwt_topic',
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
            "min_temp": 40.0,
            "max_temp": 230.0,
            "temp_step": 1,
            "modes": [STATE_HEAT, STATE_OFF],
            "fan_modes": [FAN_MODE_AUTO],
            "device": device,
        }

        _LOGGER.info(payload)
        _LOGGER.info(self.format_discovery_topic(mac, 'volcano', SENSOR_CLIMATE))

        msg = MqttConfigMessage(
            MqttConfigMessage.CLIMATE,
            self.format_discovery_topic(mac, 'volcano', SENSOR_CLIMATE),
            payload=payload,
            retain=True
        )
        msg.topic = "{}/{}".format('homeassistant', msg.topic)

        mqtt.publish([msg])

        async def status_update():
            while not self._stop_event.is_set():
                _LOGGER.info("VOLCANO UPDATE STATUS")
                mqtt.publish([
                    MqttMessage(topic=self.format_topic('volcano', SENSOR_CURRENT_TEMPERATURE), payload=volcano.temperature),
                    MqttMessage(topic=self.format_topic('volcano', SENSOR_TARGET_TEMPERATURE), payload=volcano.target_temperature),
                    MqttMessage(topic=self.format_topic('volcano', SENSOR_HEATER_STATE), payload=self.true_false_to_ha_on_off(volcano.heater_on)),
                    MqttMessage(topic=self.format_topic('volcano', SENSOR_PUMP_STATE), payload=self.true_false_to_ha_on_off(volcano.pump_on)),
                    MqttMessage(topic=self.format_topic('volcano', "fan"), payload=self.true_false_to_ha_on_off(volcano.pump_on)),
                    MqttMessage(topic=self.format_topic('volcano', "mode"), payload="heat" if volcano.heater_on else "off"),
                    MqttMessage(topic=self.format_topic('volcano', 'temperature_unit'), payload=volcano.temperature_unit),
                    MqttMessage(topic=self.format_topic('volcano', 'serial_number'), payload=volcano.serial_number),
                    MqttMessage(topic=self.format_topic('volcano', 'led_display'), payload=self.true_false_to_ha_on_off(True)),
                    MqttMessage(topic=self.format_topic('volcano', 'led_display', 'brightness'), payload=3),
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

    def _on_volcano_target_temperature(self, volcano, value: str):
        temperature = round(float(value))
        asyncio.run_coroutine_threadsafe(volcano.set_target_temperature(temperature), self._loop).result(10.0)
        return [MqttMessage(topic=self.format_topic('volcano', SENSOR_TARGET_TEMPERATURE), payload=temperature)]

    def _on_volcano_mode(self, volcano):
        return []

    def _on_volcano_fan(self, volcano, value: str):
        state = True if value == 'ON' else False
        asyncio.run_coroutine_threadsafe(volcano.set_heater(state), self._loop).result(10.0)

        return [MqttMessage(topic=self.format_topic('volcano', SENSOR_HEATER_STATE), payload=value)]

    def _on_volcano_heater(self, volcano, value: str):
        state = True if value == 'ON' else False
        asyncio.run_coroutine_threadsafe(volcano.set_heater(state), self._loop).result(10.0)

        return [MqttMessage(topic=self.format_topic('volcano', SENSOR_HEATER_STATE), payload=value)]

    def _on_volcano_pump(self, volcano, value: str):
        return []

    def _on_volcano_led_brightness(self, volcano, value: str):
        brightness = round(float(value))
        asyncio.run_coroutine_threadsafe(volcano.set_led_brightness(brightness), self._loop).result(10.0)

        return [MqttMessage(topic=self.format_topic('volcano', 'led_display', 'brightness'), payload=brightness)]
    
    def _on_volcano_temperature_unit(self, volcano, value: str):
        asyncio.run_coroutine_threadsafe(volcano.set_temperature_unit(value), self._loop).result(10.0)

        return [MqttMessage(topic=self.format_topic('volcano', 'temperature_unit'), payload=value)]

    def _on_any(self, path: str, value: str):
        return []

    def on_command(self, topic, value):
        from werkzeug.routing import Map, Rule

        asyncio.set_event_loop(self._loop)

        topic_without_prefix = topic.replace("{}/".format(self.topic_prefix), '', 1)

        route_map = Map([
            Rule('/<volcano>/mode/set', endpoint=self._on_volcano_mode.__name__),
            Rule('/<volcano>/fan/set', endpoint=self._on_volcano_fan.__name__),
            Rule('/<volcano>/heater/set', endpoint=self._on_volcano_heater.__name__),
            Rule('/<volcano>/pump/set', endpoint=self._on_volcano_pump.__name__),
            Rule('/<volcano>/led_display/brightness/set', endpoint=self._on_volcano_led_brightness.__name__),
            Rule('/<volcano>/temperature_unit/set', endpoint=self._on_volcano_temperature_unit.__name__),
            Rule('/<path:path>', endpoint=self._on_any.__name__),
        ])

        router = route_map.bind('example.com/volcano/', '/')

        (callable, kwargs) = router.match(topic_without_prefix)

        if 'volcano' in kwargs:
            if kwargs['volcano'] in self.devices:
                volcano = self.devices[kwargs['volcano']]['volcano']

                if not volcano.is_connected:
                    asyncio.run_coroutine_threadsafe(volcano.connect(), self._loop).result(10.0)

                kwargs['volcano'] = volcano
            else:
                logger.log_exception(_LOGGER, "Ignore command because device %s is unknown", kwargs['volcano'])
                return []

        kwargs['value'] = value.decode('utf-8')

        return getattr(self, callable)(**kwargs)
