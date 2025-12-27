/**
 * Home Assistant MQTT Discovery
 *
 * Publishes discovery messages for automatic device detection in Home Assistant
 * Reference: https://www.home-assistant.io/integrations/mqtt/#mqtt-discovery
 *
 * Discovery Topic Format:
 * <discovery_prefix>/<component>/[<node_id>/]<object_id>/config
 *
 * Example:
 * homeassistant/climate/nest_02AA01AC/thermostat/config
 */

import * as mqtt from 'mqtt';
import type { DeviceStateService } from '../../services/DeviceStateService';
import { resolveDeviceName } from './helpers';

/**
 * Build Home Assistant discovery payload for climate entity (main thermostat control)
 */
export function buildClimateDiscovery(
  serial: string,
  deviceName: string,
  topicPrefix: string
): any {
  // Always use Celsius - HA handles display conversion based on user preferences
  return {
    unique_id: `nolongerevil_${serial}`,
    name: deviceName,
    object_id: `nest_${serial}`,
    device: {
      identifiers: [`nolongerevil_${serial}`],
      name: deviceName,
      model: 'Nest Thermostat',
      manufacturer: 'Google Nest',
      sw_version: 'NoLongerEvil',
    },
    availability: {
      topic: `${topicPrefix}/${serial}/availability`,
      payload_available: 'online',
      payload_not_available: 'offline',
    },
    temperature_unit: 'C',
    precision: 0.5,
    temp_step: 0.5,
    current_temperature_topic: `${topicPrefix}/${serial}/ha/current_temperature`,
    current_humidity_topic: `${topicPrefix}/${serial}/ha/current_humidity`,
    temperature_command_topic: `${topicPrefix}/${serial}/ha/target_temperature/set`,
    temperature_state_topic: `${topicPrefix}/${serial}/ha/target_temperature`,
    temperature_high_command_topic: `${topicPrefix}/${serial}/ha/target_temperature_high/set`,
    temperature_high_state_topic: `${topicPrefix}/${serial}/ha/target_temperature_high`,
    temperature_low_command_topic: `${topicPrefix}/${serial}/ha/target_temperature_low/set`,
    temperature_low_state_topic: `${topicPrefix}/${serial}/ha/target_temperature_low`,
    mode_command_topic: `${topicPrefix}/${serial}/ha/mode/set`,
    mode_state_topic: `${topicPrefix}/${serial}/ha/mode`,
    modes: ['off', 'heat', 'cool', 'heat_cool'],
    action_topic: `${topicPrefix}/${serial}/ha/action`,
    fan_mode_command_topic: `${topicPrefix}/${serial}/ha/fan_mode/set`,
    fan_mode_state_topic: `${topicPrefix}/${serial}/ha/fan_mode`,
    fan_modes: ['auto', 'on'],
    preset_mode_command_topic: `${topicPrefix}/${serial}/ha/preset/set`,
    preset_mode_state_topic: `${topicPrefix}/${serial}/ha/preset`,
    preset_modes: ['home', 'away', 'eco'],
    min_temp: 9,
    max_temp: 32,
    optimistic: false,
    qos: 1,
  };
}

/**
 * Build Home Assistant discovery payload for the Humidifier
 */
export function buildHumidifierDiscovery(
  serial: string,
  deviceName: string,
  topicPrefix: string
): any {
  return {
    unique_id: `nolongerevil_${serial}_humidifier`,
    name: `${deviceName} Humidifier`,
    object_id: `nest_${serial}_humidifier`,
    device: {
      identifiers: [`nolongerevil_${serial}`],
      name: deviceName,
    },
    availability: {
      topic: `${topicPrefix}/${serial}/availability`,
      payload_available: 'online',
      payload_not_available: 'offline',
    },
    // The On/Off Switch
    command_topic: `${topicPrefix}/${serial}/ha/humidifier_enabled/set`,
    state_topic: `${topicPrefix}/${serial}/ha/humidifier_enabled`,
    payload_on: 'true',
    payload_off: 'false',
    
    // The Slider
    target_humidity_command_topic: `${topicPrefix}/${serial}/ha/target_humidity/set`,
    target_humidity_state_topic: `${topicPrefix}/${serial}/ha/target_humidity`,
    min_humidity: 10,
    max_humidity: 60,
    
    // Status (Current reading & Action)
    current_humidity_topic: `${topicPrefix}/${serial}/ha/current_humidity`,
    // Action topic determines if the icon is Blue (Humidifying) or Grey (Idle)
    action_topic: `${topicPrefix}/${serial}/ha/humidifier_action`,
    
    device_class: 'humidifier',
    origin: {
        name: 'NoLongerEvil',
        sw_version: '1.0.0'
    },
    qos: 1
  };
}

/**
 * Build Home Assistant discovery payload for temperature sensor
 */
export function buildTemperatureSensorDiscovery(
  serial: string,
  topicPrefix: string
): any {
  return {
    unique_id: `nolongerevil_${serial}_temperature`,
    name: `Temperature`,
    object_id: `nest_${serial}_temperature`,
    device: { identifiers: [`nolongerevil_${serial}`] },
    state_topic: `${topicPrefix}/${serial}/ha/current_temperature`,
    unit_of_measurement: '°C',
    device_class: 'temperature',
    state_class: 'measurement',
    availability: {
      topic: `${topicPrefix}/${serial}/availability`,
      payload_available: 'online',
      payload_not_available: 'offline',
    },
    qos: 0,
  };
}

/**
 * Build Home Assistant discovery payload for humidity sensor
 */
export function buildHumiditySensorDiscovery(
  serial: string,
  topicPrefix: string
): any {
  return {
    unique_id: `nolongerevil_${serial}_humidity`,
    name: `Humidity`,
    object_id: `nest_${serial}_humidity`,
    device: { identifiers: [`nolongerevil_${serial}`] },
    state_topic: `${topicPrefix}/${serial}/ha/current_humidity`,
    unit_of_measurement: '%',
    device_class: 'humidity',
    state_class: 'measurement',
    availability: {
      topic: `${topicPrefix}/${serial}/availability`,
      payload_available: 'online',
      payload_not_available: 'offline',
    },
    qos: 0,
  };
}

/**
 * Build Home Assistant discovery payload for outdoor temperature sensor
 */
export function buildOutdoorTemperatureSensorDiscovery(
  serial: string,
  topicPrefix: string
): any {
  return {
    unique_id: `nolongerevil_${serial}_outdoor_temperature`,
    name: `Outdoor Temperature`,
    object_id: `nest_${serial}_outdoor_temperature`,
    device: { identifiers: [`nolongerevil_${serial}`] },
    state_topic: `${topicPrefix}/${serial}/ha/outdoor_temperature`,
    unit_of_measurement: '°C',
    device_class: 'temperature',
    state_class: 'measurement',
    availability: {
      topic: `${topicPrefix}/${serial}/availability`,
      payload_available: 'online',
      payload_not_available: 'offline',
    },
    qos: 0,
  };
}

/**
 * Build Home Assistant discovery payload for occupancy binary sensor
 */
export function buildOccupancyBinarySensorDiscovery(
  serial: string,
  topicPrefix: string
): any {
  return {
    unique_id: `nolongerevil_${serial}_occupancy`,
    name: `Occupancy`,
    object_id: `nest_${serial}_occupancy`,
    device: { identifiers: [`nolongerevil_${serial}`] },
    state_topic: `${topicPrefix}/${serial}/ha/occupancy`,
    payload_on: 'home',
    payload_off: 'away',
    device_class: 'occupancy',
    availability: {
      topic: `${topicPrefix}/${serial}/availability`,
      payload_available: 'online',
      payload_not_available: 'offline',
    },
    qos: 0,
  };
}

/**
 * Build Home Assistant discovery payload for fan binary sensor
 */
export function buildFanBinarySensorDiscovery(
  serial: string,
  topicPrefix: string
): any {
  return {
    unique_id: `nolongerevil_${serial}_fan`,
    name: `Fan`,
    object_id: `nest_${serial}_fan`,
    device: { identifiers: [`nolongerevil_${serial}`] },
    state_topic: `${topicPrefix}/${serial}/ha/fan_running`,
    payload_on: 'true',
    payload_off: 'false',
    device_class: 'running',
    availability: {
      topic: `${topicPrefix}/${serial}/availability`,
      payload_available: 'online',
      payload_not_available: 'offline',
    },
    qos: 0,
  };
}

/**
 * Build Home Assistant discovery payload for leaf (eco) binary sensor
 */
export function buildLeafBinarySensorDiscovery(
  serial: string,
  topicPrefix: string
): any {
  return {
    unique_id: `nolongerevil_${serial}_leaf`,
    name: `Eco Mode`,
    object_id: `nest_${serial}_leaf`,
    device: { identifiers: [`nolongerevil_${serial}`] },
    state_topic: `${topicPrefix}/${serial}/ha/eco`,
    payload_on: 'true',
    payload_off: 'false',
    device_class: 'power',
    availability: {
      topic: `${topicPrefix}/${serial}/availability`,
      payload_available: 'online',
      payload_not_available: 'offline',
    },
    qos: 0,
  };
}

/**
 * Publish all discovery messages for a thermostat
 */
export async function publishThermostatDiscovery(
  client: mqtt.MqttClient,
  serial: string,
  deviceState: DeviceStateService,
  topicPrefix: string,
  discoveryPrefix: string
): Promise<void> {
  try {
    const deviceName = await resolveDeviceName(serial, deviceState);
    console.log(`[HA Discovery] Publishing discovery for ${serial} (${deviceName})`);

    // Climate
    const climateConfig = buildClimateDiscovery(serial, deviceName, topicPrefix);
    await publishDiscoveryMessage(
      client,
      `${discoveryPrefix}/climate/nest_${serial}/thermostat/config`,
      climateConfig
    );

    // Humidifier (New)
    const humidifierConfig = buildHumidifierDiscovery(serial, deviceName, topicPrefix);
    await publishDiscoveryMessage(
      client,
      `${discoveryPrefix}/humidifier/nest_${serial}/humidifier/config`,
      humidifierConfig
    );

    // Sensors
    const tempConfig = buildTemperatureSensorDiscovery(serial, topicPrefix);
    await publishDiscoveryMessage(
      client,
      `${discoveryPrefix}/sensor/nest_${serial}/temperature/config`,
      tempConfig
    );

    const humidityConfig = buildHumiditySensorDiscovery(serial, topicPrefix);
    await publishDiscoveryMessage(
      client,
      `${discoveryPrefix}/sensor/nest_${serial}/humidity/config`,
      humidityConfig
    );

    const outdoorTempConfig = buildOutdoorTemperatureSensorDiscovery(serial, topicPrefix);
    await publishDiscoveryMessage(
      client,
      `${discoveryPrefix}/sensor/nest_${serial}/outdoor_temperature/config`,
      outdoorTempConfig
    );

    // Binary Sensors
    const occupancyConfig = buildOccupancyBinarySensorDiscovery(serial, topicPrefix);
    await publishDiscoveryMessage(
      client,
      `${discoveryPrefix}/binary_sensor/nest_${serial}/occupancy/config`,
      occupancyConfig
    );

    const fanConfig = buildFanBinarySensorDiscovery(serial, topicPrefix);
    await publishDiscoveryMessage(
      client,
      `${discoveryPrefix}/binary_sensor/nest_${serial}/fan/config`,
      fanConfig
    );

    const leafConfig = buildLeafBinarySensorDiscovery(serial, topicPrefix);
    await publishDiscoveryMessage(
      client,
      `${discoveryPrefix}/binary_sensor/nest_${serial}/leaf/config`,
      leafConfig
    );

    console.log(`[HA Discovery] Successfully published all discovery messages for ${serial}`);
  } catch (error) {
    console.error(`[HA Discovery] Error publishing discovery for ${serial}:`, error);
    throw error;
  }
}

/**
 * Publish a single discovery message (with error handling)
 */
async function publishDiscoveryMessage(
  client: mqtt.MqttClient,
  topic: string,
  config: any
): Promise<void> {
  return new Promise((resolve, reject) => {
    const payload = typeof config === 'string' ? config : JSON.stringify(config);
    client.publish(topic, payload, { retain: true, qos: 1 }, (err) => {
      if (err) {
        console.error(`[HA Discovery] Failed to publish to ${topic}:`, err);
        reject(err);
      } else {
        resolve();
      }
    });
  });
}

/**
 * Remove discovery messages for a device (when device is removed)
 */
export async function removeDeviceDiscovery(
  client: mqtt.MqttClient,
  serial: string,
  discoveryPrefix: string
): Promise<void> {
  const topics = [
    `${discoveryPrefix}/climate/nest_${serial}/thermostat/config`,
    `${discoveryPrefix}/humidifier/nest_${serial}/humidifier/config`, // Added
    `${discoveryPrefix}/sensor/nest_${serial}/temperature/config`,
    `${discoveryPrefix}/sensor/nest_${serial}/humidity/config`,
    `${discoveryPrefix}/sensor/nest_${serial}/outdoor_temperature/config`,
    `${discoveryPrefix}/binary_sensor/nest_${serial}/occupancy/config`,
    `${discoveryPrefix}/binary_sensor/nest_${serial}/fan/config`,
    `${discoveryPrefix}/binary_sensor/nest_${serial}/leaf/config`,
  ];

  for (const topic of topics) {
    await publishDiscoveryMessage(client, topic, '');
  }

  console.log(`[HA Discovery] Removed all discovery messages for ${serial}`);
}
