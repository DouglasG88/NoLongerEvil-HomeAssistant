/**
 * Integration Framework Types
 */

export interface IntegrationConfig {
  userId: string;
  type: string;
  enabled: boolean;
  config: Record<string, any>;
}

export interface MqttConfig {
  brokerUrl: string;
  username?: string;
  password?: string;
  clientId?: string;
  topicPrefix?: string;
  discoveryPrefix?: string;
  publishRaw?: boolean;
  homeAssistantDiscovery?: boolean;
}

export interface DeviceStateChange {
  serial: string;
  objectKey: string;
  objectRevision: number;
  objectTimestamp: number;
  value: any;
}
