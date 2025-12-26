/**
 * Device Serial Number Parsing Utilities
 * Extracts and validates device serial numbers from HTTP headers
 */

import { IncomingMessage } from 'http';

/**
 * Extract serial from HTTP Basic Auth header
 * Format: Authorization: Basic base64(username:password)
 * Serial is extracted from username (may be prefixed with "nest.")
 */
function extractSerialFromAuthHeader(req: IncomingMessage): string | null {
  const authHeader = req.headers.authorization;
  if (!authHeader || !authHeader.startsWith('Basic ')) {
    return null;
  }

  try {
    const base64Credentials = authHeader.slice('Basic '.length);
    const credentials = Buffer.from(base64Credentials, 'base64').toString('utf-8');
    const [username] = credentials.split(':');

    if (!username) {
      return null;
    }

    let serial = username;
    if (serial.includes('.')) {
      const parts = serial.split('.');
      serial = parts[1] || parts[0];
    }

    return sanitizeSerial(serial);
  } catch (error) {
    console.error('[SerialParser] Failed to parse Basic Auth:', error);
    return null;
  }
}

/**
 * Extract serial from custom X-NL-Device-Serial header
 */
function extractSerialFromCustomHeader(req: IncomingMessage): string | null {
  const serialHeader = req.headers['x-nl-device-serial'];
  if (!serialHeader) {
    return null;
  }

  const serial = Array.isArray(serialHeader) ? serialHeader[0] : serialHeader;
  return sanitizeSerial(serial);
}

/**
 * Sanitize and validate serial number
 * - Uppercase
 * - Remove non-alphanumeric characters
 * - Validate minimum length (10 characters)
 */
export function sanitizeSerial(serial: string | undefined | null): string | null {
  if (!serial) {
    return null;
  }

  const cleaned = serial.toUpperCase().replace(/[^A-Z0-9]/g, '');

  if (cleaned.length < 10) {
    return null;
  }

  return cleaned;
}

/**
 * Resolve device serial from HTTP request
 * Tries Basic Auth first, then X-NL-Device-Serial header
 */
export function resolveDeviceSerial(req: IncomingMessage): string | null {
  const authSerial = extractSerialFromAuthHeader(req);
  if (authSerial) {
    return authSerial;
  }

  return extractSerialFromCustomHeader(req);
}

/**
 * Extract Weave Device ID from request headers
 * Used for device identification in Nest protocol
 */
export function extractWeaveDeviceId(req: IncomingMessage): string | null {
  const weaveId = req.headers['x-nl-weave-device-id'];
  if (!weaveId) {
    return null;
  }

  return Array.isArray(weaveId) ? weaveId[0] : weaveId;
}

/**
 * Validate that serial meets minimum requirements
 */
export function isValidSerial(serial: string | null | undefined): boolean {
  if (!serial) {
    return false;
  }

  if (!/^[A-Z0-9]+$/.test(serial)) {
    return false;
  }

  if (serial.length < 10) {
    return false;
  }

  return true;
}
