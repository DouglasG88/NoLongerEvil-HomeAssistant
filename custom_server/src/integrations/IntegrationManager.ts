/**
 * Integration Manager
 *
 * Manages lifecycle of all integrations (MQTT, WebSocket, Webhook, etc.)
 * Broadcasts device events to active integrations
 */

import { BaseIntegration } from './BaseIntegration';
import { DeviceStateChange } from './types';
import { DeviceStateService } from '../services/DeviceStateService';
import { SubscriptionManager } from '../services/SubscriptionManager';
import { AbstractDeviceStateManager } from '@/services/AbstractDeviceStateManager';

export class IntegrationManager {
  private integrations: Map<string, BaseIntegration> = new Map();
  private deviceState: DeviceStateService | null = null;
  private subscriptionManager: SubscriptionManager | null = null;
  private watchInterval: NodeJS.Timeout | null = null;
  private currentIntegrationConfigs: Map<string, any> = new Map();
  private deviceStateManager: AbstractDeviceStateManager | null = null;

  /**
   * Initialize integration manager and load all enabled integrations
   */
  async initialize(deviceStateManager: AbstractDeviceStateManager, deviceState: DeviceStateService, subscriptionManager: SubscriptionManager): Promise<void> {
    this.deviceStateManager = deviceStateManager;
    this.deviceState = deviceState;
    this.subscriptionManager = subscriptionManager;

    console.log('[IntegrationManager] Initializing...');

    try {
      // Load all enabled MQTT integrations from device state manager
      const mqttIntegrations = await this.deviceStateManager.getAllEnabledMqttIntegrations();

      console.log(`[IntegrationManager] Found ${mqttIntegrations.length} enabled MQTT integrations`);

      // Initialize each MQTT integration
      for (const { userId, config } of mqttIntegrations) {
        await this.loadMqttIntegration(userId, config);
      }

      console.log('[IntegrationManager] Initialization complete');

      this.startWatching();
    } catch (error) {
      console.error('[IntegrationManager] Failed to initialize:', error);
    }
  }

  private startWatching(): void {
    if (this.watchInterval) {
      clearInterval(this.watchInterval);
    }

    this.watchInterval = setInterval(async () => {
      await this.checkForChanges();
    }, 10000);

    console.log('[IntegrationManager] Started watching for integration changes (polling every 10s)');
  }

  private async checkForChanges(): Promise<void> {
    if (!this.deviceStateManager) return;

    try {
      const mqttIntegrations = await this.deviceStateManager.getAllEnabledMqttIntegrations();

      const newConfigs = new Map<string, any>();
      const enabledUserIds = new Set<string>();

      for (const { userId, config } of mqttIntegrations) {
        const key = `mqtt:${userId}`;
        enabledUserIds.add(userId);
        newConfigs.set(key, { userId, config, enabled: true });
      }

      const currentKeys = new Set(this.integrations.keys());
      const newKeys = new Set(newConfigs.keys());

      for (const key of currentKeys) {
        if (!newKeys.has(key)) {
          console.log(`[IntegrationManager] Integration ${key} was disabled, shutting down...`);
          const integration = this.integrations.get(key);
          if (integration) {
            await integration.shutdown();
            this.integrations.delete(key);
            this.currentIntegrationConfigs.delete(key);
          }
        }
      }

      for (const [key, data] of newConfigs) {
        const { userId, config } = data;
        const existing = this.integrations.get(key);

        if (!existing) {
          console.log(`[IntegrationManager] New integration ${key} detected, loading...`);
          await this.loadMqttIntegration(userId, config);
          this.currentIntegrationConfigs.set(key, config);
        } else {
          const oldConfig = this.currentIntegrationConfigs.get(key);
          if (JSON.stringify(oldConfig) !== JSON.stringify(config)) {
            console.log(`[IntegrationManager] Integration ${key} config changed, reloading...`);
            await existing.shutdown();
            this.integrations.delete(key);
            await this.loadMqttIntegration(userId, config);
            this.currentIntegrationConfigs.set(key, config);
          }
        }
      }
    } catch (error) {
      console.error('[IntegrationManager] Error checking for integration changes:', error);
    }
  }

  /**
   * Load a single MQTT integration
   */
  private async loadMqttIntegration(userId: string, config: any): Promise<void> {
    try {
      // Lazy load MQTT integration to avoid loading mqtt library if not used
      const { MqttIntegration } = await import('./mqtt/MqttIntegration');

      if (!this.deviceStateManager || !this.deviceState || !this.subscriptionManager) {
        throw new Error('IntegrationManager not initialized');
      }

      const integration = new MqttIntegration(userId, config, this.deviceState, this.deviceStateManager, this.subscriptionManager);
      await integration.initialize();

      const key = `mqtt:${userId}`;
      this.integrations.set(key, integration);

      console.log(`[IntegrationManager] Loaded MQTT integration for user ${userId}`);
    } catch (error) {
      console.error(`[IntegrationManager] Failed to load MQTT integration for user ${userId}:`, error);
    }
  }

  /**
   * Register an integration manually (for testing or future use)
   */
  registerIntegration(key: string, integration: BaseIntegration): void {
    this.integrations.set(key, integration);
  }

  /**
   * Notify all integrations of a device state change
   */
  async notifyStateChange(serial: string, objectKey: string, objectRevision: number, objectTimestamp: number, value: any): Promise<void> {
    const change: DeviceStateChange = {
      serial,
      objectKey,
      objectRevision,
      objectTimestamp,
      value,
    };

    // Broadcast to all integrations concurrently
    const promises = Array.from(this.integrations.values()).map(async (integration) => {
      try {
        await integration.onDeviceStateChange(change);
      } catch (error) {
        console.error(`[IntegrationManager] Error in ${integration.getType()} integration for user ${integration.getUserId()}:`, error);
      }
    });

    await Promise.all(promises);
  }

  /**
   * Notify integrations when a device connects
   */
  async notifyDeviceConnected(serial: string): Promise<void> {
    const promises = Array.from(this.integrations.values()).map(async (integration) => {
      try {
        await integration.onDeviceConnected(serial);
      } catch (error) {
        console.error(`[IntegrationManager] Error notifying device connection:`, error);
      }
    });

    await Promise.all(promises);
  }

  /**
   * Notify integrations when a device disconnects
   */
  async notifyDeviceDisconnected(serial: string): Promise<void> {
    const promises = Array.from(this.integrations.values()).map(async (integration) => {
      try {
        await integration.onDeviceDisconnected(serial);
      } catch (error) {
        console.error(`[IntegrationManager] Error notifying device disconnection:`, error);
      }
    });

    await Promise.all(promises);
  }

  /**
   * Notify integrations of device availability change
   */
  async notifyAvailabilityChange(serial: string, isAvailable: boolean): Promise<void> {
    console.log(`[IntegrationManager] Device ${serial} availability changed: ${isAvailable ? 'AVAILABLE' : 'UNAVAILABLE'}`);
    
    const promises = Array.from(this.integrations.values()).map(async (integration) => {
      try {
        if (isAvailable) {
          await integration.onDeviceConnected(serial);
        } else {
          await integration.onDeviceDisconnected(serial);
        }
      } catch (error) {
        console.error(`[IntegrationManager] Error notifying availability change for ${serial}:`, error);
      }
    });

    await Promise.all(promises);
  }

  /**
   * Reload integrations from database (hot reload)
   */
  async reload(): Promise<void> {
    console.log('[IntegrationManager] Reloading integrations...');

    // Shutdown all existing integrations
    await this.shutdown();

    // Reinitialize
    if (this.deviceStateManager && this.deviceState && this.subscriptionManager) {
      await this.initialize(this.deviceStateManager, this.deviceState, this.subscriptionManager);
    }
  }

  /**
   * Shutdown all integrations
   */
  async shutdown(): Promise<void> {
    console.log('[IntegrationManager] Shutting down all integrations...');

    if (this.watchInterval) {
      clearInterval(this.watchInterval);
      this.watchInterval = null;
    }

    const promises = Array.from(this.integrations.values()).map(async (integration) => {
      try {
        await integration.shutdown();
      } catch (error) {
        console.error(`[IntegrationManager] Error shutting down integration:`, error);
      }
    });

    await Promise.all(promises);
    this.integrations.clear();
    this.currentIntegrationConfigs.clear();

    console.log('[IntegrationManager] All integrations shut down');
  }

  /**
   * Get count of active integrations
   */
  getActiveCount(): number {
    return this.integrations.size;
  }

  /**
   * Get integration types (for status/debugging)
   */
  getActiveIntegrationTypes(): string[] {
    return Array.from(this.integrations.values()).map((i) => i.getType());
  }
}
