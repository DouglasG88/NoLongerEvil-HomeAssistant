/**
 * Temperature Safety Business Logic
 *
 * Enforces safety bounds on temperature setpoints to prevent:
 * - Freeze conditions (too low)
 * - Overheating (too high)
 *
 * Default bounds: 7.222°C (45°F) to 35°C (95°F)
 */

import type { TemperatureSafetyBounds } from '../lib/types';

/**
 * Default safety bounds (in Celsius)
 */
const DEFAULT_LOWER_SAFETY = 7.222; // 45°F
const DEFAULT_UPPER_SAFETY = 35.0;   // 95°F

/**
 * Extract safety bounds from device/shared object value
 * Falls back to defaults if not present
 */
export function getSafetyBounds(value: Record<string, any>): TemperatureSafetyBounds {
  return {
    lower: typeof value.lower_safety_temp === 'number' ? value.lower_safety_temp : DEFAULT_LOWER_SAFETY,
    upper: typeof value.upper_safety_temp === 'number' ? value.upper_safety_temp : DEFAULT_UPPER_SAFETY,
  };
}

/**
 * Clamp temperature to safety bounds
 * Returns clamped value and whether clamping occurred
 */
export function clampTemperature(
  temperature: number,
  bounds: TemperatureSafetyBounds
): { value: number; clamped: boolean } {
  const original = temperature;
  const clamped = Math.max(bounds.lower, Math.min(bounds.upper, temperature));

  if (clamped !== original) {
    console.warn(
      `[TemperatureSafety] Clamped temperature ${original}°C to ${clamped}°C ` +
      `(bounds: ${bounds.lower}°C - ${bounds.upper}°C)`
    );
    return { value: clamped, clamped: true };
  }

  return { value: clamped, clamped: false };
}

/**
 * Validate and clamp temperature setpoint
 * Used for control commands from web dashboard
 */
export function validateTemperature(
  temperature: number,
  deviceValue: Record<string, any>
): number {
  const bounds = getSafetyBounds(deviceValue);
  const result = clampTemperature(temperature, bounds);
  return result.value;
}
