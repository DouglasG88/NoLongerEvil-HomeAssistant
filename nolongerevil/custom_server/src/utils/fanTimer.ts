/**
 * Fan Timer Business Logic
 *
 * Preserves active fan timer state when device sends stale updates.
 * Critical protocol requirement: if fan timer is active (timeout > now),
 * preserve all fan-related fields even if device doesn't include them.
 * 
 * UNLESS the device is explicitly turning the fan off.
 */

import type { FanTimerState } from '../lib/types';

/**
 * Check if incoming value is explicitly turning off the fan
 * This takes precedence over preservation logic
 */
export function isExplicitlyTurningOffFan(incomingValue: Record<string, any>): boolean {
  // If fan_timer_timeout is explicitly set to 0, the device is turning off the fan
  if ('fan_timer_timeout' in incomingValue && incomingValue.fan_timer_timeout === 0) {
    return true;
  }
  
  // If fan_control_state is explicitly set to false, the device is turning off the fan
  if ('fan_control_state' in incomingValue && incomingValue.fan_control_state === false) {
    return true;
  }
  
  return false;
}

/**
 * Check if fan timer fields should be preserved
 * Returns true if existing state has active fan timer
 */
export function shouldPreserveFanTimer(
  existingValue: Record<string, any>,
  incomingValue: Record<string, any>
): boolean {
  // Never preserve if device is explicitly turning off the fan
  if (isExplicitlyTurningOffFan(incomingValue)) {
    return false;
  }

  const fanTimerTimeout = existingValue.fan_timer_timeout;

  if (typeof fanTimerTimeout !== 'number') {
    return false;
  }

  const nowSeconds = Math.floor(Date.now() / 1000);
  return fanTimerTimeout > nowSeconds && fanTimerTimeout !== 0;
}

/**
 * Extract fan timer state from existing value
 */
export function extractFanTimerState(value: Record<string, any>): FanTimerState {
  return {
    fan_timer_timeout: value.fan_timer_timeout,
    fan_control_state: value.fan_control_state,
    fan_timer_duration: value.fan_timer_duration,
    fan_current_speed: value.fan_current_speed,
    fan_mode: value.fan_mode,
  };
}

/**
 * Merge fan timer state into value
 * Preserves active fan timer fields from existing state
 */
export function preserveFanTimer(
  mergedValue: Record<string, any>,
  existingValue: Record<string, any>
): Record<string, any> {
  if (!shouldPreserveFanTimer(existingValue, mergedValue)) {
    return mergedValue;
  }

  const fanTimerState = extractFanTimerState(existingValue);

  console.log(
    `[FanTimer] Preserving active fan timer (timeout: ${fanTimerState.fan_timer_timeout}, ` +
    `current time: ${Math.floor(Date.now() / 1000)})`
  );

  return {
    ...mergedValue,
    ...fanTimerState,
  };
}
