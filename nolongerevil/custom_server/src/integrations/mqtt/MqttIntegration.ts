/**
 * MQTT Integration
 *
 * Connects to MQTT broker and:
 * - Publishes device state changes to MQTT topics (Nest-compatible field names)
 * - Subscribes to command topics and executes commands
 * - Publishes Home Assistant discovery messages
 * - Handles availability (online/offline) status
 *
 */

import * as mqtt from 'mqtt';
import { BaseIntegration } from '../BaseIntegration';
import { DeviceStateChange, MqttConfig } from '../types';
import { DeviceStateService } from '../../services/DeviceStateService';
import { AbstractDeviceStateManager } from '@/services/AbstractDeviceStateManager';
import { SubscriptionManager } from '../../services/SubscriptionManager';
import { validateTemperature } from '../../utils/temperatureSafety';
import {
  parseObjectKey,
  buildStateTopic,
  buildAvailabilityTopic,
  parseCommandTopic,
  getCommandTopicPatterns,
} from './topicBuilder';
import { publishThermostatDiscovery, removeDeviceDiscovery } from './HomeAssistantDiscovery';
import {
  nestModeToHA,
  haModeToNest,
  deriveHvacAction,
  deriveFanMode,
  isDeviceAway,
  isFanRunning,
  isEcoActive,
  nestPresetToHA,
} from './helpers';

export class MqttIntegration extends BaseIntegration {
  private client: mqtt.MqttClient | null = null;
  private config: MqttConfig;
  private deviceState: DeviceStateService;
  private deviceStateManager: AbstractDeviceStateManager;
  private subscriptionManager: SubscriptionManager;
  private userDeviceSerials: Set<string> = new Set();
  private isReady: boolean = false;
  private deviceWatchInterval: NodeJS.Timeout | null = null;

  constructor(
    userId: string,
    config: MqttConfig,
    deviceState: DeviceStateService,
    deviceStateManager: AbstractDeviceStateManager,
    subscriptionManager: SubscriptionManager
  ) {
    super(userId, 'mqtt');
    this.config = {
      topicPrefix: 'nolongerevil', // Default to your hardware logs
      discoveryPrefix: 'homeassistant',
      clientId: `nolongerevil-${userId}`,
      publishRaw: true, 
      homeAssistantDiscovery: true, // Default to true so it works out of the box
      ...config,
    };

    // Strict overrides to ensure compatibility
    this.config.topicPrefix = 'nolongerevil';
    this.config.homeAssistantDiscovery = true;

    this.deviceState = deviceState;
    this.deviceStateManager = deviceStateManager;
    this.subscriptionManager = subscriptionManager;
  }

  /**
   * Initialize MQTT connection
   */
  async initialize(): Promise<void> {
    console.log(`[MQTT:${this.userId}] Initializing MQTT integration...`);

    try {
      await this.loadUserDevices();
      await this.connectToBroker();

      // --- CRITICAL WAIT LOOP ---
      // Ensures the connection is stable before sending data
      if (this.client) {
        let attempts = 0;
        const maxAttempts = 50; // 5s max wait
        while (!this.client.connected && attempts < maxAttempts) {
          await new Promise((resolve) => setTimeout(resolve, 100));
          attempts++;
        }
        
        if (!this.client.connected) {
          console.warn(`[MQTT:${this.userId}] Warning: Proceeding without confirmed connection.`);
        } else {
          console.log(`[MQTT:${this.userId}] Connection confirmed after ${attempts * 100}ms`);
        }
      }

      await this.subscribeToCommands();
      await this.publishDiscoveryMessages();
      await this.publishInitialState();

      this.startDeviceWatching();

      this.isReady = true;
      console.log(`[MQTT:${this.userId}] Integration initialized successfully`);
    } catch (error) {
      console.error(`[MQTT:${this.userId}] Failed to initialize:`, error);
      throw error;
    }
  }

  /**
   * Start polling for device changes
   */
  private startDeviceWatching(): void {
    if (this.deviceWatchInterval) {
      clearInterval(this.deviceWatchInterval);
    }

    this.deviceWatchInterval = setInterval(async () => {
      await this.checkForDeviceChanges();
    }, 10000);

    console.log(`[MQTT:${this.userId}] Started watching for device changes (polling every 10s)`);
  }

  /**
   * Check for added/removed devices and update accordingly
   */
  private async checkForDeviceChanges(): Promise<void> {
    if (!this.isReady) return;

    try {
      const ownedDevices = await this.deviceStateManager.listUserDevices(this.userId);
      const sharedDevices = await this.deviceStateManager.getSharedWithMe(this.userId);

      const currentSerials = new Set<string>();
      for (const device of ownedDevices) {
        currentSerials.add(device.serial);
      }
      for (const share of sharedDevices) {
        currentSerials.add(share.serial);
      }

      // Detect removed devices
      for (const serial of this.userDeviceSerials) {
        if (!currentSerials.has(serial)) {
          console.log(`[MQTT:${this.userId}] Device ${serial} was removed, cleaning up...`);
          await this.handleDeviceRemoved(serial);
        }
      }

      // Detect added devices
      for (const serial of currentSerials) {
        if (!this.userDeviceSerials.has(serial)) {
          console.log(`[MQTT:${this.userId}] New device ${serial} detected, publishing discovery...`);
          this.userDeviceSerials.add(serial);

          if (this.config.homeAssistantDiscovery && this.client) {
            try {
              await publishThermostatDiscovery(
                this.client,
                serial,
                this.deviceState,
                this.config.topicPrefix!,
                this.config.discoveryPrefix!
              );
              await this.publishHomeAssistantState(serial);
              await this.publishAvailability(serial, 'online');
            } catch (error) {
              console.error(`[MQTT:${this.userId}] Failed to publish discovery for new device ${serial}:`, error);
            }
          }
        }
      }
    } catch (error) {
      console.error(`[MQTT:${this.userId}] Error checking for device changes:`, error);
    }
  }

  /**
   * Handle device removal - clean up HA discovery
   */
  private async handleDeviceRemoved(serial: string): Promise<void> {
    this.userDeviceSerials.delete(serial);
    await this.publishAvailability(serial, 'offline');

    if (this.config.homeAssistantDiscovery && this.client) {
      try {
        await removeDeviceDiscovery(this.client, serial, this.config.discoveryPrefix!);
        console.log(`[MQTT:${this.userId}] Successfully removed device ${serial} from Home Assistant`);
      } catch (error) {
        console.error(`[MQTT:${this.userId}] Failed to remove discovery for ${serial}:`, error);
      }
    }
  }

  /**
   * Load user's devices from device state manager
   */
  private async loadUserDevices(): Promise<void> {
    try {
      const ownedDevices = await this.deviceStateManager.listUserDevices(this.userId);
      const sharedDevices = await this.deviceStateManager.getSharedWithMe(this.userId);

      this.userDeviceSerials.clear();
      for (const device of ownedDevices) {
        this.userDeviceSerials.add(device.serial);
      }
      for (const share of sharedDevices) {
        this.userDeviceSerials.add(share.serial);
      }

      console.log(`[MQTT:${this.userId}] Loaded ${this.userDeviceSerials.size} device(s)`);
    } catch (error) {
      console.error(`[MQTT:${this.userId}] Failed to load user devices:`, error);
    }
  }

  /**
   * Connect to MQTT broker
   */
  private async connectToBroker(): Promise<void> {
    return new Promise((resolve, reject) => {
      const options: mqtt.IClientOptions = {
        clientId: this.config.clientId,
        clean: true,
        reconnectPeriod: 5000,
        connectTimeout: 10000,
      };

      if (this.config.username) options.username = this.config.username;
      if (this.config.password) options.password = this.config.password;

      options.will = {
        topic: `${this.config.topicPrefix}/status`,
        payload: 'offline',
        qos: 1,
        retain: true,
      };

      this.client = mqtt.connect(this.config.brokerUrl!, options);

      this.client.on('connect', () => {
        console.log(`[MQTT:${this.userId}] Connected to broker ${this.config.brokerUrl}`);
        resolve();
      });

      this.client.on('error', (error) => {
        console.error(`[MQTT:${this.userId}] Connection error:`, error);
        if (!this.client!.connected) {
          reject(error);
        }
      });

      this.client.on('message', async (topic, message) => {
        await this.handleCommand(topic, message);
      });
    });
  }

  /**
   * Subscribe to command topics
   */
  private async subscribeToCommands(): Promise<void> {
    if (!this.client) return;

    const prefix = this.config.topicPrefix!;
    const patterns: string[] = [];

    if (this.config.publishRaw !== false) {
      patterns.push(...getCommandTopicPatterns(prefix));
    }

    if (this.config.homeAssistantDiscovery) {
      patterns.push(`${prefix}/+/ha/+/set`);
    }

    for (const pattern of patterns) {
      await new Promise<void>((resolve, reject) => {
        this.client!.subscribe(pattern, { qos: 1 }, (err) => {
          if (err) {
            console.error(`[MQTT:${this.userId}] Failed to subscribe to ${pattern}:`, err);
            reject(err);
          } else {
            console.log(`[MQTT:${this.userId}] Subscribed to commands: ${pattern}`);
            resolve();
          }
        });
      });
    }
  }

  /**
   * Handle incoming MQTT command
   */
  private async handleCommand(topic: string, message: Buffer): Promise<void> {
    try {
      const prefix = this.config.topicPrefix!;

      if (topic.includes('/ha/') && topic.endsWith('/set')) {
        await this.handleHomeAssistantCommand(topic, message);
        return;
      }

      const parsed = parseCommandTopic(topic, prefix);
      if (!parsed) {
        console.warn(`[MQTT:${this.userId}] Invalid command topic: ${topic}`);
        return;
      }

      const { serial, objectType, field } = parsed;

      if (!this.userDeviceSerials.has(serial)) {
        console.warn(`[MQTT:${this.userId}] Unauthorized command for device ${serial}`);
        return;
      }

      const valueStr = message.toString();
      let value: any = valueStr;

      try {
        value = JSON.parse(valueStr);
      } catch {
        const num = parseFloat(valueStr);
        if (!isNaN(num)) value = num;
      }

      console.log(`[MQTT:${this.userId}] Command: ${serial}/${objectType}.${field} = ${value}`);

      const objectKey = `${objectType}.${serial}`;
      const currentObj = await this.deviceState.get(serial, objectKey);

      if (!currentObj) {
        console.warn(`[MQTT:${this.userId}] Object not found: ${objectKey}`);
        return;
      }

      const newValue = { ...currentObj.value, [field]: value };
      const newRevision = currentObj.object_revision + 1;
      const newTimestamp = Date.now();

      await this.deviceState.upsert(serial, objectKey, newRevision, newTimestamp, newValue);
      console.log(`[MQTT:${this.userId}] Command executed successfully`);
    } catch (error) {
      console.error(`[MQTT:${this.userId}] Failed to handle command:`, error);
    }
  }

  /**
   * Handle Home Assistant formatted command
   */
  private async handleHomeAssistantCommand(topic: string, message: Buffer): Promise<void> {
    try {
      const prefix = this.config.topicPrefix!;
      const valueStr = message.toString().trim();

      const match = topic.match(new RegExp(`^${prefix.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}/([^/]+)/ha/(.+)/set$`));
      if (!match) return;

      const [, serial, command] = match;

      if (!this.userDeviceSerials.has(serial)) return;

      console.log(`[MQTT:${this.userId}] HA Command: ${serial}/${command} = ${valueStr}`);

      const deviceObj = await this.deviceState.get(serial, `device.${serial}`);
      const sharedObj = await this.deviceState.get(serial, `shared.${serial}`);

      if (!deviceObj || !sharedObj) return;

      switch (command) {
        case 'mode':
          await this.updateSharedValue(serial, sharedObj, 'target_temperature_type', haModeToNest(valueStr));
          break;

        case 'target_temperature':
          await this.updateSharedValue(
            serial, 
            sharedObj, 
            'target_temperature', 
            validateTemperature(parseFloat(valueStr), sharedObj.value)
          );
          break;

        case 'target_temperature_low':
          await this.updateSharedValue(
            serial, 
            sharedObj, 
            'target_temperature_low', 
            validateTemperature(parseFloat(valueStr), sharedObj.value)
          );
          break;

        case 'target_temperature_high':
          await this.updateSharedValue(
            serial, 
            sharedObj, 
            'target_temperature_high', 
            validateTemperature(parseFloat(valueStr), sharedObj.value)
          );
          break;

        case 'fan_mode':
          const isFanOn = valueStr === 'on';
          await this.updateDeviceFields(serial, deviceObj, {
            fan_control_state: isFanOn,
            fan_timer_active: isFanOn,
            fan_timer_timeout: isFanOn ? Math.floor(Date.now() / 1000) + 3600 : 0,
          });
          break;

        case 'preset':
          if (valueStr === 'away') {
            await this.updateDeviceFields(serial, deviceObj, { auto_away: 2, away: true });
          } else if (valueStr === 'home') {
            await this.updateDeviceFields(serial, deviceObj, { auto_away: 0, away: false });
          } else if (valueStr === 'eco') {
            await this.updateDeviceValue(serial, deviceObj, 'eco', { mode: 'manual-eco', leaf: true });
          }
          break;

        // HUMIDIFIER COMMANDS
        case 'target_humidity':
          const humVal = parseFloat(valueStr);
          if (!isNaN(humVal) && humVal >= 10 && humVal <= 60) {
            await this.updateSharedValue(serial, sharedObj, 'target_humidity', humVal);
            // If user moves slider, auto-enable
            if (deviceObj.value.target_humidity_enabled === false) {
              await this.updateDeviceValue(serial, deviceObj, 'target_humidity_enabled', true);
            }
          }
          break;

        case 'humidifier_enabled':
          const isEnabled = valueStr === 'true';
          await this.updateDeviceValue(serial, deviceObj, 'target_humidity_enabled', isEnabled);
          break;

        default:
          console.warn(`[MQTT:${this.userId}] Unknown HA command: ${command}`);
      }

      await this.publishHomeAssistantState(serial);
    } catch (error) {
      console.error(`[MQTT:${this.userId}] Failed to handle HA command:`, error);
    }
  }

  private async updateSharedValue(serial: string, currentObj: any, field: string, value: any): Promise<void> {
    const objectKey = `shared.${serial}`;
    const newValue = { ...currentObj.value, [field]: value };
    const newRevision = currentObj.object_revision + 1;
    const newTimestamp = Date.now();
    const updatedObj = await this.deviceState.upsert(serial, objectKey, newRevision, newTimestamp, newValue);
    this.subscriptionManager.notify(serial, objectKey, updatedObj);
  }

  private async updateDeviceValue(serial: string, currentObj: any, field: string, value: any): Promise<void> {
    const objectKey = `device.${serial}`;
    const newValue = { ...currentObj.value, [field]: value };
