/**
 * MQTT/Home Assistant Integration Helpers
 *
 * Utilities for:
 * - Device name resolution
 * - Temperature conversion
 * - Mode mapping (Nest ↔ Home Assistant)
 * - HVAC action derivation
 */

import type { DeviceStateService } from '../../services/DeviceStateService';

/**
 * where_id to human-readable room name mapping
 * Nest uses UUID-based where_id values
 */
const WHERE_ID_NAMES: Record<string, string> = {
  "00000000-0000-0000-0000-000100000000": "Entryway",
  "00000000-0000-0000-0000-000100000001": "Basement",
  "00000000-0000-0000-0000-000100000002": "Hallway",
  "00000000-0000-0000-0000-000100000003": "Den",
  "00000000-0000-0000-0000-000100000004": "Attic",
  "00000000-0000-0000-0000-000100000005": "Master Bedroom",
  "00000000-0000-0000-0000-000100000006": "Downstairs",
  "00000000-0000-0000-0000-000100000007": "Garage",
  "00000000-0000-0000-0000-000100000009": "Bathroom",
  "00000000-0000-0000-0000-00010000000a": "Kitchen",
  "00000000-0000-0000-0000-00010000000b": "Family Room",
  "00000000-0000-0000-0000-00010000000c": "Living Room",
  "00000000-0000-0000-0000-00010000000d": "Bedroom",
  "00000000-0000-0000-0000-00010000000e": "Office",
  "00000000-0000-0000-0000-00010000000f": "Upstairs",
  "00000000-0000-0000-0000-000100000010": "Dining Room",
  "00000000-0000-0000-0000-000100000011": "Backyard",
  "00000000-0000-0000-0000-000100000012": "Driveway",
  "00000000-0000-0000-0000-000100000013": "Front Yard",
  "00000000-0000-0000-0000-000100000014": "Outside",
  "00000000-0000-0000-0000-000100000015": "Guest House",
  "00000000-0000-0000-0000-000100000016": "Shed",
  "00000000-0000-0000-0000-000100000017": "Deck",
  "00000000-0000-0000-0000-000100000018": "Patio",
  "00000000-0000-0000-0000-00010000001a": "Guest Room",
  "00000000-0000-0000-0000-00010000001b": "Front Door",
  "00000000-0000-0000-0000-00010000001c": "Side Door",
  "00000000-0000-0000-0000-00010000001d": "Back Door",
};

/**
 * Resolve device display name using fallback hierarchy
 * Matches frontend logic: label → shared.name → where_id → serial
 */
export async function resolveDeviceName(
  serial: string,
  deviceState: DeviceStateService
): Promise<string> {
  try {
    const sharedObj = await deviceState.get(serial, `shared.${serial}`);
    const deviceObj = await deviceState.get(serial, `device.${serial}`);

    if (sharedObj?.value?.label && typeof sharedObj.value.label === 'string') {
      return sharedObj.value.label;
    }

    if (sharedObj?.value?.name && typeof sharedObj.value.name === 'string') {
      return sharedObj.value.name;
    }

    if (deviceObj?.value?.where_id && typeof deviceObj.value.where_id === 'string') {
      const whereId = deviceObj.value.where_id;
      if (WHERE_ID_NAMES[whereId]) {
        return WHERE_ID_NAMES[whereId];
      }
    }

    return serial;
  } catch (error) {
    console.error(`[MQTT Helpers] Error resolving device name for ${serial}:`, error);
    return serial;
  }
}

/**
 * Map Nest mode to Home Assistant HVAC mode
 * Nest: "off", "heat", "cool", "range"
 * HA: "off", "heat", "cool", "heat_cool"
 */
export function nestModeToHA(nestMode: string | undefined): string {
  if (!nestMode) return 'off';

  switch (nestMode) {
    case 'off':
      return 'off';
    case 'heat':
      return 'heat';
    case 'cool':
      return 'cool';
    case 'range':
    case 'heat-cool':
      return 'heat_cool';
    default:
      return 'off';
  }
}

/**
 * Map Home Assistant HVAC mode to Nest mode
 */
export function haModeToNest(haMode: string | undefined): string {
  if (!haMode) return 'off';

  switch (haMode) {
    case 'off':
      return 'off';
    case 'heat':
      return 'heat';
    case 'cool':
      return 'cool';
    case 'heat_cool':
      return 'range';
    default:
      return 'off';
  }
}

/**
 * Derive fan mode from device state
 * Returns: "on" or "auto"
 */
export async function deriveFanMode(
  serial: string,
  deviceState: DeviceStateService
): Promise<string> {
  try {
    const deviceObj = await deviceState.get(serial, `device.${serial}`);
    const device = deviceObj?.value || {};

    // Fan mode is "on" if:
    // 1. Fan timer has time remaining (fan_timer_timeout > now) - PRIORITY CHECK
    // 2. Fan control is enabled (fan_control_state from device) - PRIORITY CHECK
    // 
    // We prioritize the commanded state (fan_timer_timeout, fan_control_state) over
    // the physical state (hvac_fan_state) because the thermostat may lag behind
    // server commands by up to 2 minutes due to battery-saving update delays.
    
    const nowSeconds = Math.floor(Date.now() / 1000);
    const hasFanTimer = typeof device.fan_timer_timeout === 'number' && device.fan_timer_timeout > nowSeconds;
    
    const isFanModeOn = hasFanTimer || device.fan_control_state;
    
    return isFanModeOn ? 'on' : 'auto';
  } catch (error) {
    console.error(`[MQTT Helpers] Error deriving fan mode for ${serial}:`, error);
    return 'auto';
  }
}

/**
 * Derive HVAC action from device state
 * Returns: "off", "heating", "cooling", "fan", "idle"
 */
export async function deriveHvacAction(
  serial: string,
  deviceState: DeviceStateService
): Promise<string> {
  try {
    const sharedObj = await deviceState.get(serial, `shared.${serial}`);
    const deviceObj = await deviceState.get(serial, `device.${serial}`);
    const shared = sharedObj?.value || {};
    const device = deviceObj?.value || {};

    const mode = shared.target_temperature_type || '';
    if (mode === 'off') {
      return 'off';
    }

    // HVAC state fields are in the shared object, not device
    const isHeating =
      shared.hvac_heater_state ||
      shared.hvac_heat_x2_state ||
      shared.hvac_heat_x3_state ||
      shared.hvac_aux_heater_state ||
      shared.hvac_alt_heat_state;

    if (isHeating) {
      return 'heating';
    }

    const isCooling =
      shared.hvac_ac_state ||
      shared.hvac_cool_x2_state ||
      shared.hvac_cool_x3_state;

    if (isCooling) {
      return 'cooling';
    }

    // Fan is running if fan timer is active or fan control is enabled
    // We prioritize the commanded state (fan_timer_timeout, fan_control_state) over
    // the physical state (hvac_fan_state) because the thermostat may lag behind
    // server commands by up to 2 minutes due to battery-saving update delays.
    const nowSeconds = Math.floor(Date.now() / 1000);
    const hasFanTimer = typeof device.fan_timer_timeout === 'number' && device.fan_timer_timeout > nowSeconds;
    const isFanRunning = hasFanTimer || device.fan_control_state;
    
    if (isFanRunning) {
      return 'fan';
    }

    return 'idle';
  } catch (error) {
    console.error(`[MQTT Helpers] Error deriving HVAC action for ${serial}:`, error);
    return 'idle';
  }
}

/**
 * Check if device is in away mode
 */
export async function isDeviceAway(
  serial: string,
  deviceState: DeviceStateService
): Promise<boolean> {
  try {
    const deviceObj = await deviceState.get(serial, `device.${serial}`);
    const device = deviceObj?.value || {};

    if (typeof device.auto_away === 'number') {
      return device.auto_away > 0;
    }

    return Boolean(device.away);
  } catch (error) {
    console.error(`[MQTT Helpers] Error checking away status for ${serial}:`, error);
    return false;
  }
}

/**
 * Check if heating is active (any stage)
 */
export async function isHeatingActive(
  serial: string,
  deviceState: DeviceStateService
): Promise<boolean> {
  try {
    const sharedObj = await deviceState.get(serial, `shared.${serial}`);
    const shared = sharedObj?.value || {};

    return Boolean(
      shared.hvac_heater_state ||
      shared.hvac_heat_x2_state ||
      shared.hvac_heat_x3_state ||
      shared.hvac_aux_heater_state ||
      shared.hvac_alt_heat_state
    );
  } catch (error) {
    console.error(`[MQTT Helpers] Error checking heating status for ${serial}:`, error);
    return false;
  }
}

/**
 * Check if cooling is active (any stage)
 */
export async function isCoolingActive(
  serial: string,
  deviceState: DeviceStateService
): Promise<boolean> {
  try {
    const sharedObj = await deviceState.get(serial, `shared.${serial}`);
    const shared = sharedObj?.value || {};

    return Boolean(
      shared.hvac_ac_state ||
      shared.hvac_cool_x2_state ||
      shared.hvac_cool_x3_state
    );
  } catch (error) {
    console.error(`[MQTT Helpers] Error checking cooling status for ${serial}:`, error);
    return false;
  }
}

/**
 * Check if fan is running
 */
export async function isFanRunning(
  serial: string,
  deviceState: DeviceStateService
): Promise<boolean> {
  try {
    const sharedObj = await deviceState.get(serial, `shared.${serial}`);
    const shared = sharedObj?.value || {};

    return Boolean(shared.hvac_fan_state);
  } catch (error) {
    console.error(`[MQTT Helpers] Error checking fan status for ${serial}:`, error);
    return false;
  }
}

/**
 * Check if eco/leaf mode is active
 */
export async function isEcoActive(
  serial: string,
  deviceState: DeviceStateService
): Promise<boolean> {
  try {
    const deviceObj = await deviceState.get(serial, `device.${serial}`);
    const device = deviceObj?.value || {};

    return Boolean(device.eco?.leaf || device.leaf);
  } catch (error) {
    console.error(`[MQTT Helpers] Error checking eco status for ${serial}:`, error);
    return false;
  }
}

/**
 * Map Nest preset to Home Assistant preset
 * HA presets: "eco", "away", "home"
 */
export async function nestPresetToHA(
  serial: string,
  deviceState: DeviceStateService
): Promise<string | null> {
  try {
    const isAway = await isDeviceAway(serial, deviceState);
    if (isAway) {
      return 'away';
    }

    const isEco = await isEcoActive(serial, deviceState);
    if (isEco) {
      return 'eco';
    }

    return 'home';
  } catch (error) {
    console.error(`[MQTT Helpers] Error deriving preset for ${serial}:`, error);
    return 'home';
  }
}
