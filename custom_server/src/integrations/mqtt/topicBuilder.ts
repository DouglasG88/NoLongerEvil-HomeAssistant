/**
 * MQTT Topic Builder
 *
 * Builds Nest-compatible MQTT topic names
 * Topics use exact Nest field naming conventions:
 * - device.{serial} → {prefix}/{serial}/device/{field}
 * - shared.{serial} → {prefix}/{serial}/shared/{field}
 */

/**
 * Parse object_key into components
 * Examples:
 * - "device.02AA01AC" → { objectType: "device", serial: "02AA01AC" }
 * - "shared.02AA01AC" → { objectType: "shared", serial: "02AA01AC" }
 */
export function parseObjectKey(objectKey: string): { objectType: string; serial: string } | null {
  const parts = objectKey.split('.');
  if (parts.length < 2) {
    return null;
  }

  const objectType = parts[0]; // "device", "shared", "user", "structure", etc.
  const serial = parts.slice(1).join('.'); // Handle serials with dots

  return { objectType, serial };
}

/**
 * Build state topic for a specific field
 * Example: buildStateTopic("nolongerevil", "02AA01AC", "device", "current_temperature")
 *   → "nolongerevil/02AA01AC/device/current_temperature"
 */
export function buildStateTopic(
  prefix: string,
  serial: string,
  objectType: string,
  field?: string
): string {
  if (field) {
    return `${prefix}/${serial}/${objectType}/${field}`;
  }
  // Full object topic (JSON)
  return `${prefix}/${serial}/${objectType}`;
}

/**
 * Build command topic for a specific field
 * Example: buildCommandTopic("nolongerevil", "02AA01AC", "shared", "target_temperature")
 *   → "nolongerevil/02AA01AC/shared/target_temperature/set"
 */
export function buildCommandTopic(
  prefix: string,
  serial: string,
  objectType: string,
  field: string
): string {
  return `${prefix}/${serial}/${objectType}/${field}/set`;
}

/**
 * Build availability topic
 * Example: buildAvailabilityTopic("nolongerevil", "02AA01AC")
 *   → "nolongerevil/02AA01AC/availability"
 */
export function buildAvailabilityTopic(prefix: string, serial: string): string {
  return `${prefix}/${serial}/availability`;
}

/**
 * Parse command topic back into components
 * Example: "nolongerevil/02AA01AC/shared/target_temperature/set"
 *   → { serial: "02AA01AC", objectType: "shared", field: "target_temperature" }
 */
export function parseCommandTopic(
  topic: string,
  prefix: string
): { serial: string; objectType: string; field: string } | null {
  // Remove prefix
  if (!topic.startsWith(prefix + '/')) {
    return null;
  }

  const withoutPrefix = topic.substring(prefix.length + 1);
  const parts = withoutPrefix.split('/');

  // Expected format: {serial}/{objectType}/{field}/set
  if (parts.length < 4 || parts[parts.length - 1] !== 'set') {
    return null;
  }

  const serial = parts[0];
  const objectType = parts[1];
  const field = parts.slice(2, -1).join('/'); // Handle fields with slashes

  return { serial, objectType, field };
}

/**
 * Get all command topic patterns for subscription
 * Returns wildcard patterns like: "nolongerevil/+/shared/+/set"
 */
export function getCommandTopicPatterns(prefix: string): string[] {
  return [
    `${prefix}/+/device/+/set`,   // Device object commands
    `${prefix}/+/shared/+/set`,   // Shared object commands
  ];
}
