/**
 * Control API Status Routes
 * - /status - Inspect device state
 * - /api/devices - List all devices
 * - /notify-device - Manual notification trigger
 */

import { IncomingMessage, ServerResponse } from 'http';
import * as url from 'url';
import { DeviceStateService } from '../../services/DeviceStateService';
import { SubscriptionManager } from '../../services/SubscriptionManager';

/**
 * Handle GET /status
 * Returns device state for debugging/dashboard
 */
export function handleStatus(
  req: IncomingMessage,
  res: ServerResponse,
  deviceState: DeviceStateService
): void {
  const parsedUrl = url.parse(req.url || '', true);
  const serial = parsedUrl.query.serial as string | undefined;

  const allState = deviceState.getAllState();
  const devices = Object.keys(allState);

  const response = {
    devices,
    deviceState: allState,
    requestedSerial: serial || null,
  };

  res.writeHead(200, { 'Content-Type': 'application/json' });
  res.end(JSON.stringify(response));
}

/**
 * Handle GET /api/devices
 * Returns list of all known devices
 */
export function handleDevices(
  _req: IncomingMessage,
  res: ServerResponse,
  deviceState: DeviceStateService
): void {
  const allState = deviceState.getAllState();

  const devices = Object.keys(allState).map(serial => ({
    serial,
    objects: Object.keys(allState[serial]),
  }));

  res.writeHead(200, { 'Content-Type': 'application/json' });
  res.end(JSON.stringify(devices));
}

/**
 * Handle POST /notify-device
 * Manually trigger notification for subscribers
 */
export async function handleNotifyDevice(
  body: any,
  deviceState: DeviceStateService,
  subscriptionManager: SubscriptionManager
): Promise<{ success: boolean; notified: number; error?: string }> {
  const { serial, objectKeys } = body;

  if (!serial || !Array.isArray(objectKeys)) {
    throw new Error('Missing required fields: serial, objectKeys');
  }

  let totalNotified = 0;

  for (const objectKey of objectKeys) {
    const obj = await deviceState.get(serial, objectKey);
    if (!obj) {
      console.warn(`[NotifyDevice] Object not found: ${serial}/${objectKey}`);
      continue;
    }

    const result = subscriptionManager.notify(serial, objectKey, obj);
    totalNotified += result.notified;
  }

  return {
    success: true,
    notified: totalNotified,
  };
}
