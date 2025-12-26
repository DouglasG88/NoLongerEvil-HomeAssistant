/**
 * Base Integration Abstract Class
 *
 * All integrations (MQTT, WebSocket, Webhook, etc.) extend this class
 */

import { DeviceStateChange } from './types';

export abstract class BaseIntegration {
  protected userId: string;
  protected type: string;

  constructor(userId: string, type: string) {
    this.userId = userId;
    this.type = type;
  }

  /**
   * Initialize the integration (connect to services, etc.)
   */
  abstract initialize(): Promise<void>;

  /**
   * Shutdown the integration (disconnect, cleanup resources)
   */
  abstract shutdown(): Promise<void>;

  /**
   * Called when device state changes
   */
  abstract onDeviceStateChange(change: DeviceStateChange): Promise<void>;

  /**
   * Called when a device connects (first subscription)
   */
  abstract onDeviceConnected(serial: string): Promise<void>;

  /**
   * Called when a device disconnects (subscription timeout)
   */
  abstract onDeviceDisconnected(serial: string): Promise<void>;

  /**
   * Get user ID this integration belongs to
   */
  getUserId(): string {
    return this.userId;
  }

  /**
   * Get integration type
   */
  getType(): string {
    return this.type;
  }
}
