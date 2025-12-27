/**
 * Home Assistant MQTT Discovery
 *
 * Publishes discovery messages for automatic device detection in Home Assistant
 * Reference: https://www.home-assistant.io/integrations/mqtt/#mqtt-discovery
 */

import * as mqtt from 'mqtt';
import type { DeviceStateService } from '../../services/DeviceStateService';
import { resolveDeviceName } from './helpers';

/**
 * Build Home Assistant discovery payload for climate entity
 */
export function buildClimateDiscovery(
  serial: string,
  deviceName: string,
  topicPrefix: string
): any {
  return {
    unique_id: `nolongerevil_${serial}`,
    name: deviceName,
    default_entity_id: `climate.nest_${serial}`,
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
 * Build Home Assistant discovery payload for Humidifier
 */
export function buildHumidifierDiscovery(
  serial: string,
  deviceName: string,
  topicPrefix: string
): any {
  return {
    unique_id: `nolongerevil_${serial}_humidifier`,
    name: `${deviceName} Humidifier`,
    default_entity_id: `humidifier.nest_${serial}_humidifier`,
    device: {
      identifiers: [`nolongerevil_${serial}`],
      name: deviceName,
    },
    availability: {
      topic: `${topicPrefix}/${serial}/availability`,
      payload_available: 'online',
      payload_not_available: 'offline',
    },
    // 1. Switch (On/Off) - Uses simple boolean state
    command_topic: `${topicPrefix}/${serial}/ha/humidifier_state/set`,
    state_topic: `${topicPrefix}/${serial}/ha/humidifier_state`,
    payload_on: 'true',
    payload_off: 'false',
    
    // 2. Slider (Target) - Handled by smart HA logic
    target_humidity_command_topic: `${topicPrefix}/${serial}/ha/target_humidity/set`,
    target_humidity_state_topic: `${topicPrefix}/${serial}/device/target_humidity`,
    min_humidity: 10,
    max_humidity: 60,
    
    // 3. Status
    action_topic: `${topicPrefix}/${serial}/ha/humidifier_action`,
    current_humidity_topic: `${topicPrefix}/${serial}/ha/current_humidity`,
    
    device_class: 'humidifier',
    origin: {
        name: 'NoLongerEvil',
        sw_version: '1.0.0'
    },
    qos: 1
  };
}

export function buildTemperatureSensorDiscovery(serial: string, topicPrefix: string): any {
  return {
    unique_id: `nolongerevil_${serial}_temperature`,
    name: `Temperature`,
    default_entity_id: `sensor.nest_${serial}_temperature`,
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

export function buildHumiditySensorDiscovery(serial: string, topicPrefix: string): any {
  return {
    unique_id: `nolongerevil_${serial}_humidity`,
    name: `Humidity`,
    default_entity_id: `sensor.nest_${serial}_humidity`,
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

export function buildOutdoorTemperatureSensorDiscovery(serial: string, topicPrefix: string): any {
  return {
    unique_id: `nolongerevil_${serial}_outdoor_temperature`,
    name: `Outdoor Temperature`,
    default_entity_id: `sensor.nest_${serial}_outdoor_temperature`,
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

export function buildOccupancyBinarySensorDiscovery(serial: string, topicPrefix: string): any {
  return {
    unique_id: `nolongerevil_${serial}_occupancy`,
    name: `Occupancy`,
    default_entity_id: `binary_sensor.nest_${serial}_occupancy`,
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

export function buildFanBinarySensorDiscovery(serial: string, topicPrefix: string): any {
  return {
    unique_id: `nolongerevil_${serial}_fan`,
    name: `Fan`,
    default_entity_id: `binary_sensor.nest_${serial}_fan`,
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

export function buildLeafBinarySensorDiscovery(serial: string, topicPrefix: string): any {
  return {
    unique_id: `nolongerevil_${serial}_leaf`,
    name: `Eco Mode`,
    default_entity_id: `binary_sensor.nest_${serial}_leaf`,
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

    const deviceObj = await deviceState.get(serial, `device.${serial}`);
    const hasHumidifier = deviceObj?.value?.has_humidifier === true;

    await publishDiscoveryMessage(client, `${discoveryPrefix}/climate/nest_${serial}/thermostat/config`, buildClimateDiscovery(serial, deviceName, topicPrefix));

    if (hasHumidifier) {
      await publishDiscoveryMessage(client, `${discoveryPrefix}/humidifier/nest_${serial}/humidifier/config`, buildHumidifierDiscovery(serial, deviceName, topicPrefix));
    }

    await publishDiscoveryMessage(client, `${discoveryPrefix}/sensor/nest_${serial}/temperature/config`, buildTemperatureSensorDiscovery(serial, topicPrefix));
    await publishDiscoveryMessage(client, `${discoveryPrefix}/sensor/nest_${serial}/humidity/config`, buildHumiditySensorDiscovery(serial, topicPrefix));
    await publishDiscoveryMessage(client, `${discoveryPrefix}/sensor/nest_${serial}/outdoor_temperature/config`, buildOutdoorTemperatureSensorDiscovery(serial, topicPrefix));
    await publishDiscoveryMessage(client, `${discoveryPrefix}/binary_sensor/nest_${serial}/occupancy/config`, buildOccupancyBinarySensorDiscovery(serial, topicPrefix));
    await publishDiscoveryMessage(client, `${discoveryPrefix}/binary_sensor/nest_${serial}/fan/config`, buildFanBinarySensorDiscovery(serial, topicPrefix));
    await publishDiscoveryMessage(client, `${discoveryPrefix}/binary_sensor/nest_${serial}/leaf/config`, buildLeafBinarySensorDiscovery(serial, topicPrefix));

    console.log(`[HA Discovery] Successfully published all discovery messages for ${serial}`);
  } catch (error) {
    console.error(`[HA Discovery] Error publishing discovery for ${serial}:`, error);
    throw error;
  }
}

async function publishDiscoveryMessage(client: mqtt.MqttClient, topic: string, config: any): Promise<void> {
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

export async function removeDeviceDiscovery(client: mqtt.MqttClient, serial: string, discoveryPrefix: string): Promise<void> {
  const topics = [
    `${discoveryPrefix}/climate/nest_${serial}/thermostat/config`,
    `${discoveryPrefix}/humidifier/nest_${serial}/humidifier/config`,
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
