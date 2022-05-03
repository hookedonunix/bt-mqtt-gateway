
from curses import KEY_A1
import logger
import asyncio
import threading

from mqtt import MqttMessage, MqttConfigMessage
from workers.base import BaseWorker, retry

_LOGGER = logger.get(__name__)

REQUIREMENTS = ["git+https://github.com/hookedonunix/volcano-bt.git#egg=volcanobt"]

STATE_HEAT = "heat"
STATE_OFF = "off"

FAN_ON = "on"
FAN_OFF = "off"

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
            "fan_modes": [FAN_ON, FAN_OFF],
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
                MqttMessage(topic=self.format_topic('volcano', "mode"), payload="heat" if state else "off")
            ])

        volcano.on_heater_changed(heater_changed)

        def pump_changed(state):
            _LOGGER.info(f"Pump changed to {state}")
            mqtt.publish([
                MqttMessage(topic=self.format_topic('volcano', "fan"), payload=self.true_false_to_ha_on_off(state))
            ])

        volcano.on_pump_changed(pump_changed)

        _LOGGER.info("Before connect")
        await volcano.connect()
        _LOGGER.info("After connect")
        
        _LOGGER.info("Initializing metrics")
        await volcano.initialize_metrics()

        #mqtt.publish([
        #    MqttMessage(topic=self.format_topic('volcano', SENSOR_CURRENT_TEMPERATURE), payload=volcano.temperature),
        #    MqttMessage(topic=self.format_topic('volcano', SENSOR_TARGET_TEMPERATURE), payload=volcano.target_temperature),
        #    MqttMessage(topic=self.format_topic('volcano', SENSOR_HEATER_STATE), payload=self.true_false_to_ha_on_off(volcano.heater_on)),
        #    MqttMessage(topic=self.format_topic('volcano', SENSOR_PUMP_STATE), payload=self.true_false_to_ha_on_off(volcano.pump_on)),
        #    MqttMessage(topic=self.format_topic('volcano', "fan"), payload=self.true_false_to_ha_on_off(volcano.pump_on)),
        #    MqttMessage(topic=self.format_topic('volcano', "mode"), payload="heat" if volcano.heater_on else "off"),
        #])

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
        asyncio.set_event_loop(self._loop)

        _LOGGER.info(topic)
        _LOGGER.info(value)

        ret = []
        topic_without_prefix = topic.replace("{}/".format(self.topic_prefix), '', 1)
        device_name, method, _ = topic_without_prefix.split("/")

        if device_name in self.devices:
            data = self.devices[device_name]
            volcano = data["volcano"]
        else:
            logger.log_exception(_LOGGER, "Ignore command because device %s is unknown", device_name)
            return []

        if (not volcano.is_connected):
            future = asyncio.run_coroutine_threadsafe(volcano.connect(), self._loop)
            future.result(10.0)

        value = value.decode("utf-8")

        if method == "mode":
            state = True if value == STATE_HEAT else False
            asyncio.run_coroutine_threadsafe(volcano.set_heater(state), self._loop)
            ret.append(MqttMessage(topic=self.format_topic('volcano', "mode"), payload=value))

        if method == "fan":
            state = True if value == FAN_ON else False
            asyncio.run_coroutine_threadsafe(volcano.set_pump(state), self._loop)
            ret.append(MqttMessage(topic=self.format_topic('volcano', "fan"), payload=value))

        if method == SENSOR_HEATER_STATE:
            state = True if value == 'ON' else False
            future = asyncio.run_coroutine_threadsafe(volcano.set_heater(state), self._loop)
            _LOGGER.info(value)
            _LOGGER.info(future.result(10.0))
            ret.append(MqttMessage(topic=self.format_topic('volcano', SENSOR_HEATER_STATE), payload=value))
        elif method == SENSOR_PUMP_STATE:
            state = True if value == 'ON' else False
            asyncio.run_coroutine_threadsafe(volcano.set_pump(state), self._loop)
            ret.append(MqttMessage(topic=self.format_topic('volcano', SENSOR_PUMP_STATE), payload=value))
        elif method == SENSOR_TARGET_TEMPERATURE:
            temperature = round(float(value))
            _LOGGER.info(temperature)
            asyncio.run_coroutine_threadsafe(volcano.set_target_temperature(temperature), self._loop)
            _LOGGER.info("Coroutine run")
            ret.append(MqttMessage(topic=self.format_topic('volcano', SENSOR_TARGET_TEMPERATURE), payload=temperature))
            _LOGGER.info("After coroutine run")

        return ret
