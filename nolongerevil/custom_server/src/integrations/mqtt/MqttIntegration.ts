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
      topicPrefix: 'nolongerevil',
      discoveryPrefix: 'homeassistant',
      clientId: `nolongerevil-${userId}`,
      publishRaw: true, 
      homeAssistantDiscovery: true,
      ...config,
    };
    
    // Force overrides
    this.config.topicPrefix = 'nolongerevil';
    this.config.homeAssistantDiscovery = true;

    this.deviceState = deviceState;
    this.deviceStateManager = deviceStateManager;
    this.subscriptionManager = subscriptionManager;
  }

  async initialize(): Promise<void> {
    console.log(`[MQTT:${this.userId}] Initializing MQTT integration...`);

    try {
      await this.loadUserDevices();
      await this.connectToBroker();

      // --- WAIT LOOP ---
      if (this.client) {
        let attempts = 0;
        const maxAttempts = 50; 
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

  private startDeviceWatching(): void {
    if (this.deviceWatchInterval) clearInterval(this.deviceWatchInterval);
    this.deviceWatchInterval = setInterval(async () => {
      await this.checkForDeviceChanges();
    }, 10000);
    console.log(`[MQTT:${this.userId}] Started watching for device changes (polling every 10s)`);
  }

  private async checkForDeviceChanges(): Promise<void> {
    if (!this.isReady) return;
    try {
      const ownedDevices = await this.deviceStateManager.listUserDevices(this.userId);
      const sharedDevices = await this.deviceStateManager.getSharedWithMe(this.userId);

      const currentSerials = new Set<string>();
      for (const device of ownedDevices) currentSerials.add(device.serial);
      for (const share of sharedDevices) currentSerials.add(share.serial);

      for (const serial of this.userDeviceSerials) {
        if (!currentSerials.has(serial)) await this.handleDeviceRemoved(serial);
      }

      for (const serial of currentSerials) {
        if (!this.userDeviceSerials.has(serial)) {
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

  private async handleDeviceRemoved(serial: string): Promise<void> {
    this.userDeviceSerials.delete(serial);
    await this.publishAvailability(serial, 'offline');
    if (this.config.homeAssistantDiscovery && this.client) {
      try {
        await removeDeviceDiscovery(this.client, serial, this.config.discoveryPrefix!);
      } catch (error) {
        console.error(`[MQTT:${this.userId}] Failed to remove discovery for ${serial}:`, error);
      }
    }
  }

  private async loadUserDevices(): Promise<void> {
    try {
      const ownedDevices = await this.deviceStateManager.listUserDevices(this.userId);
      const sharedDevices = await this.deviceStateManager.getSharedWithMe(this.userId);
      this.userDeviceSerials.clear();
      for (const device of ownedDevices) this.userDeviceSerials.add(device.serial);
      for (const share of sharedDevices) this.userDeviceSerials.add(share.serial);
    } catch (error) {
      console.error(`[MQTT:${this.userId}] Failed to load user devices:`, error);
    }
  }

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
      this.client.on('connect', () => resolve());
      this.client.on('error', (error) => {
        if (!this.client!.connected) reject(error);
      });
      this.client.on('message', async (topic, message) => {
        await this.handleCommand(topic, message);
      });
    });
  }

  private async subscribeToCommands(): Promise<void> {
    if (!this.client) return;
    const prefix = this.config.topicPrefix!;
    const patterns = [];
    if (this.config.publishRaw !== false) patterns.push(...getCommandTopicPatterns(prefix));
    if (this.config.homeAssistantDiscovery) patterns.push(`${prefix}/+/ha/+/set`);
    for (const pattern of patterns) {
      await new Promise<void>((resolve, reject) => {
        this.client!.subscribe(pattern, { qos: 1 }, (err) => {
          if (err) reject(err); else resolve();
        });
      });
    }
  }

  private async handleCommand(topic: string, message: Buffer): Promise<void> {
    try {
      const prefix = this.config.topicPrefix!;
      if (topic.includes('/ha/') && topic.endsWith('/set')) {
        await this.handleHomeAssistantCommand(topic, message);
        return;
      }
      const parsed = parseCommandTopic(topic, prefix);
      if (!parsed) return;
      const { serial, objectType, field } = parsed;
      if (!this.userDeviceSerials.has(serial)) return;
      const valueStr = message.toString();
      let value: any = valueStr;
      try { value = JSON.parse(valueStr); } catch {
        const num = parseFloat(valueStr);
        if (!isNaN(num)) value = num;
      }
      const objectKey = `${objectType}.${serial}`;
      const currentObj = await this.deviceState.get(serial, objectKey);
      if (!currentObj) return;
      const newValue = { ...currentObj.value, [field]: value };
      await this.deviceState.upsert(serial, objectKey, currentObj.object_revision + 1, Date.now(), newValue);
    } catch (error) {
      console.error(`[MQTT:${this.userId}] Failed to handle command:`, error);
    }
  }

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
          await this.updateSharedValue(serial, sharedObj, 'target_temperature', validateTemperature(parseFloat(valueStr), sharedObj.value));
          break;
        case 'target_temperature_low':
          await this.updateSharedValue(serial, sharedObj, 'target_temperature_low', validateTemperature(parseFloat(valueStr), sharedObj.value));
          break;
        case 'target_temperature_high':
          await this.updateSharedValue(serial, sharedObj, 'target_temperature_high', validateTemperature(parseFloat(valueStr), sharedObj.value));
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

        // --- UPDATED HUMIDIFIER LOGIC (Enable-First Order) ---
        case 'target_humidity':
          let humVal = parseFloat(valueStr);
          if (!isNaN(humVal) && humVal >= 10 && humVal <= 60) {
            // 1. ROUNDING
            humVal = Math.round(humVal / 5) * 5;
            
            console.log(`[MQTT:${this.userId}] Set Humidity Sequence: Force Enable -> Set ${humVal}%`);

            // 2. FORCE ENABLE FIRST (Shared is critical for hardware)
            await this.updateSharedValue(serial, sharedObj, 'target_humidity_enabled', true);
            await this.updateDeviceValue(serial, deviceObj, 'target_humidity_enabled', true);

            // 3. SET VALUE SECOND
            await this.updateSharedValue(serial, sharedObj, 'target_humidity', humVal);
            await this.updateDeviceValue(serial, deviceObj, 'target_humidity', humVal);
          }
          break;

        // UPDATED: Using 'target_humidity_enabled' to match discovery
        case 'target_humidity_enabled':
        case 'humidifier_enabled': // Keep for backward compat
          const isEnabled = valueStr === 'true';
          
          console.log(`[MQTT:${this.userId}] Setting Humidifier Enabled: ${isEnabled}`);
          await this.updateSharedValue(serial, sharedObj, 'target_humidity_enabled', isEnabled);
          await this.updateDeviceValue(serial, deviceObj, 'target_humidity_enabled', isEnabled);

          if (isEnabled) {
            const currentTgt = sharedObj.value.target_humidity;
            if (currentTgt === undefined || currentTgt < 0) {
                console.log(`[MQTT:${this.userId}] Humidifier ON -> Force 40% default`);
                await this.updateSharedValue(serial, sharedObj, 'target_humidity', 40);
            }
          }
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
    const updatedObj = await this.deviceState.upsert(serial, objectKey, currentObj.object_revision + 1, Date.now(), newValue);
    this.subscriptionManager.notify(serial, objectKey, updatedObj);
  }

  private async updateDeviceValue(serial: string, currentObj: any, field: string, value: any): Promise<void> {
    const objectKey = `device.${serial}`;
    const newValue = { ...currentObj.value, [field]: value };
    const updatedObj = await this.deviceState.upsert(serial, objectKey, currentObj.object_revision + 1, Date.now(), newValue);
    this.subscriptionManager.notify(serial, objectKey, updatedObj);
  }

  private async updateDeviceFields(serial: string, currentObj: any, fields: Record<string, any>): Promise<void> {
    const objectKey = `device.${serial}`;
    const newValue = { ...currentObj.value, ...fields };
    const updatedObj = await this.deviceState.upsert(serial, objectKey, currentObj.object_revision + 1, Date.now(), newValue);
    this.subscriptionManager.notify(serial, objectKey, updatedObj);
  }

  private async publishDiscoveryMessages(): Promise<void> {
    if (!this.client || !this.config.homeAssistantDiscovery) return;
    for (const serial of this.userDeviceSerials) {
      try {
        await publishThermostatDiscovery(
          this.client,
          serial,
          this.deviceState,
          this.config.topicPrefix!,
          this.config.discoveryPrefix!
        );
      } catch (error) {
        console.error(`[MQTT:${this.userId}] Failed to publish discovery for ${serial}:`, error);
      }
    }
  }

  private async publishInitialState(): Promise<void> {
    if (!this.client) return;
    for (const serial of this.userDeviceSerials) {
      try {
        const deviceObjects = await this.deviceState.getAllForDevice(serial);
        const objectKeys = Object.keys(deviceObjects);
        if (objectKeys.length === 0) continue;
        for (const objectKey of objectKeys) {
          const obj = deviceObjects[objectKey];
          await this.publishObjectState(serial, objectKey, obj.value, true); 
        }
        if (this.config.homeAssistantDiscovery) await this.publishHomeAssistantState(serial);
        await this.publishAvailability(serial, 'online');
      } catch (error) {
        console.error(`[MQTT:${this.userId}] Failed to publish initial state for ${serial}:`, error);
      }
    }
  }

  private async publishObjectState(serial: string, objectKey: string, value: any, skipHA: boolean = false): Promise<void> {
    if (!this.client || !this.isReady) return;
    const parsed = parseObjectKey(objectKey);
    if (!parsed) return;
    if (this.config.publishRaw !== false) {
      const fullTopic = buildStateTopic(this.config.topicPrefix!, serial, parsed.objectType);
      await this.publish(fullTopic, JSON.stringify(value), { retain: true, qos: 0 });
      for (const [field, fieldValue] of Object.entries(value)) {
        const fieldTopic = buildStateTopic(this.config.topicPrefix!, serial, parsed.objectType, field);
        const payload = typeof fieldValue === 'object' ? JSON.stringify(fieldValue) : String(fieldValue);
        await this.publish(fieldTopic, payload, { retain: true, qos: 0 });
      }
    }
    if (this.config.homeAssistantDiscovery && !skipHA) await this.publishHomeAssistantState(serial);
  }

  private async publishHomeAssistantState(serial: string): Promise<void> {
    if (!this.client || !this.isReady) return;
    try {
      const prefix = this.config.topicPrefix!;
      const deviceObj = await this.deviceState.get(serial, `device.${serial}`);
      const sharedObj = await this.deviceState.get(serial, `shared.${serial}`);
      if (!deviceObj || !sharedObj) return;

      const device = deviceObj.value || {};
      const shared = sharedObj.value || {};

      const currentTemp = shared.current_temperature ?? device.current_temperature;
      if (currentTemp !== undefined) await this.publish(`${prefix}/${serial}/ha/current_temperature`, String(currentTemp), { retain: true, qos: 0 });
      if (device.current_humidity !== undefined) await this.publish(`${prefix}/${serial}/ha/current_humidity`, String(device.current_humidity), { retain: true, qos: 0 });

      // --- HUMIDIFIER STATE ---
      const targetHum = device.target_humidity ?? shared.target_humidity;
      if (targetHum !== undefined && targetHum >= 0) {
        await this.publish(`${prefix}/${serial}/ha/target_humidity`, String(targetHum), { retain: true, qos: 0 });
      }

      // Enabled State: From SHARED
      const isEnabled = shared.target_humidity_enabled === true;
      // Publish to 'target_humidity_enabled' topic
      await this.publish(`${prefix}/${serial}/ha/target_humidity_enabled`, String(isEnabled), { retain: true, qos: 0 });

      // Action / Valve State: From DEVICE
      const valveState = String(device.humidifier_state).toLowerCase();
      const isValveOpen = valveState === 'true'; 
      let humAction = 'idle';
      if (!isEnabled) {
        humAction = 'off';
      } else if (isValveOpen) {
        humAction = 'humidifying';
      }
      await this.publish(`${prefix}/${serial}/ha/humidifier_action`, humAction, { retain: true, qos: 0 });

      if (shared.target_temperature !== undefined) await this.publish(`${prefix}/${serial}/ha/target_temperature`, String(shared.target_temperature), { retain: true, qos: 0 });
      if (shared.target_temperature_low !== undefined) await this.publish(`${prefix}/${serial}/ha/target_temperature_low`, String(shared.target_temperature_low), { retain: true, qos: 0 });
      if (shared.target_temperature_high !== undefined) await this.publish(`${prefix}/${serial}/ha/target_temperature_high`, String(shared.target_temperature_high), { retain: true, qos: 0 });

      await this.publish(`${prefix}/${serial}/ha/mode`, nestModeToHA(shared.target_temperature_type), { retain: true, qos: 0 });
      await this.publish(`${prefix}/${serial}/ha/action`, await deriveHvacAction(serial, this.deviceState), { retain: true, qos: 0 });
      await this.publish(`${prefix}/${serial}/ha/fan_mode`, await deriveFanMode(serial, this.deviceState), { retain: true, qos: 0 });
      
      const preset = await nestPresetToHA(serial, this.deviceState);
      if (preset) await this.publish(`${prefix}/${serial}/ha/preset`, preset, { retain: true, qos: 0 });

      let outdoorTempCelsius = device.outdoor_temperature ?? shared.outside_temperature ?? device.outside_temperature;
      if (outdoorTempCelsius === undefined) {
        try {
          const userWeather = await this.deviceStateManager.getUserWeather(this.userId);
          if (userWeather?.current?.temp_c !== undefined) outdoorTempCelsius = userWeather.current.temp_c;
        } catch (e) {}
      }
      if (outdoorTempCelsius !== undefined) await this.publish(`${prefix}/${serial}/ha/outdoor_temperature`, String(outdoorTempCelsius), { retain: true, qos: 0 });

      const isAway = await isDeviceAway(serial, this.deviceState);
      await this.publish(`${prefix}/${serial}/ha/occupancy`, isAway ? 'away' : 'home', { retain: true, qos: 0 });
      const fanRunning = await isFanRunning(serial, this.deviceState);
      await this.publish(`${prefix}/${serial}/ha/fan_running`, String(fanRunning), { retain: true, qos: 0 });
      const eco = await isEcoActive(serial, this.deviceState);
      await this.publish(`${prefix}/${serial}/ha/eco`, String(eco), { retain: true, qos: 0 });

      console.log(`[MQTT:${this.userId}] Successfully published HA state for ${serial}`);
    } catch (error) {
      console.error(`[MQTT:${this.userId}] Error publishing HA state:`, error);
    }
  }

  private async publishAvailability(serial: string, status: 'online' | 'offline'): Promise<void> {
    if (!this.client) return;
    const topic = buildAvailabilityTopic(this.config.topicPrefix!, serial);
    await this.publish(topic, status, { retain: true, qos: 1 });
  }

  private async publish(topic: string, message: string, options: mqtt.IClientPublishOptions): Promise<void> {
    if (!this.client) return;
    return new Promise((resolve, reject) => {
      this.client!.publish(topic, message, options, (err) => {
        if (err) reject(err); else resolve();
      });
    });
  }

  async onDeviceStateChange(change: DeviceStateChange): Promise<void> {
    if (!this.userDeviceSerials.has(change.serial)) return;
    await this.publishObjectState(change.serial, change.objectKey, change.value);
  }

  async onDeviceConnected(serial: string): Promise<void> {
    if (this.userDeviceSerials.has(serial)) await this.publishAvailability(serial, 'online');
  }

  async onDeviceDisconnected(serial: string): Promise<void> {
    if (this.userDeviceSerials.has(serial)) await this.publishAvailability(serial, 'offline');
  }

  async shutdown(): Promise<void> {
    if (this.deviceWatchInterval) clearInterval(this.deviceWatchInterval);
    if (this.client) {
      for (const serial of this.userDeviceSerials) await this.publishAvailability(serial, 'offline');
      await new Promise<void>((resolve) => this.client!.end(false, {}, () => resolve()));
      this.client = null;
    }
    this.isReady = false;
  }
}
