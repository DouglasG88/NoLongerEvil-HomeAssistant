/**
 * Device Availability Watchdog
 *
 * Tracks when devices were last seen and marks them as unavailable
 * if they haven't communicated in more than 125 seconds.
 *
 * Devices are considered "seen" when they:
 * - Hit /nest/entry
 * - Send a PUT request to /nest/transport/put
 * - Have an active SUBSCRIBE connection (long-polling)
 *
 * All devices start as UNAVAILABLE until first activity.
 */

import { SubscriptionManager } from './SubscriptionManager';

export interface DeviceAvailabilityState {
  serial: string;
  lastSeen: number; // Unix timestamp in milliseconds
  isAvailable: boolean;
}

export class DeviceAvailabilityWatchdog {
  private deviceStates: Map<string, DeviceAvailabilityState> = new Map();
  private watchdogTimer: NodeJS.Timeout | null = null;
  private readonly TIMEOUT_MS = 300000; // 5 minutes (300 seconds)
  private readonly CHECK_INTERVAL_MS = 30000; // Check every 30 seconds
  private onAvailabilityChange: ((serial: string, isAvailable: boolean) => void) | null = null;
  private subscriptionManager: SubscriptionManager | null = null;

  /**
   * Start the watchdog timer
   */
  start(subscriptionManager: SubscriptionManager): void {
    if (this.watchdogTimer) {
      console.warn('[DeviceAvailability] Watchdog already running');
      return;
    }

    this.subscriptionManager = subscriptionManager;

    console.log(`[DeviceAvailability] Starting watchdog (timeout: ${this.TIMEOUT_MS}ms, check: ${this.CHECK_INTERVAL_MS}ms)`);

    this.watchdogTimer = setInterval(() => {
      this.checkDeviceTimeouts();
    }, this.CHECK_INTERVAL_MS);
  }

  /**
   * Stop the watchdog timer
   */
  stop(): void {
    if (this.watchdogTimer) {
      clearInterval(this.watchdogTimer);
      this.watchdogTimer = null;
      console.log('[DeviceAvailability] Watchdog stopped');
    }
  }

  /**
   * Set callback for availability changes
   */
  setAvailabilityChangeHandler(callback: (serial: string, isAvailable: boolean) => void): void {
    this.onAvailabilityChange = callback;
  }

  /**
   * Mark a device as seen (heartbeat)
   */
  markSeen(serial: string): void {
    const now = Date.now();
    const existingState = this.deviceStates.get(serial);

    if (!existingState) {
      // First time seeing this device
      console.log(`[${new Date().toISOString()}] [DeviceAvailability] Device ${serial} first seen`);
      this.deviceStates.set(serial, {
        serial,
        lastSeen: now,
        isAvailable: true,
      });

      // Notify that device is now available
      this.notifyAvailabilityChange(serial, true);
    } else {
      // Update last seen timestamp
      const wasAvailable = existingState.isAvailable;
      existingState.lastSeen = now;

      if (!wasAvailable) {
        // Device came back online
        console.log(`[${new Date().toISOString()}] [DeviceAvailability] Device ${serial} came back online`);
        existingState.isAvailable = true;
        this.notifyAvailabilityChange(serial, true);
      }
    }
  }

  /**
   * Check all devices for timeouts
   */
  private checkDeviceTimeouts(): void {
    const now = Date.now();

    // Also check devices with active subscriptions (keep them alive)
    if (this.subscriptionManager) {
      const activeSerials = this.subscriptionManager.getActiveSerials();
      for (const serial of activeSerials) {
        // Mark devices with active subscriptions as seen
        const state = this.deviceStates.get(serial);
        if (state) {
          state.lastSeen = now; // Keep updating while subscription is active
          
          // If was unavailable, mark as available now
          if (!state.isAvailable) {
            console.log(`[${new Date().toISOString()}] [DeviceAvailability] Device ${serial} has active subscription, marking available`);
            state.isAvailable = true;
            this.notifyAvailabilityChange(serial, true);
          }
        } else {
          // First time seeing this device via subscription
          console.log(`[${new Date().toISOString()}] [DeviceAvailability] Device ${serial} first seen (active subscription)`);
          this.deviceStates.set(serial, {
            serial,
            lastSeen: now,
            isAvailable: true,
          });
          this.notifyAvailabilityChange(serial, true);
        }
      }
    }

    // Check for timeouts
    for (const [serial, state] of this.deviceStates.entries()) {
      if (!state.isAvailable) {
        // Already marked unavailable, skip
        continue;
      }

      const timeSinceLastSeen = now - state.lastSeen;

      if (timeSinceLastSeen > this.TIMEOUT_MS) {
        console.log(`[${new Date().toISOString()}] [DeviceAvailability] Device ${serial} timed out (last seen ${Math.round(timeSinceLastSeen / 1000)}s ago)`);
        state.isAvailable = false;
        this.notifyAvailabilityChange(serial, false);
      }
    }
  }

  /**
   * Notify callback of availability change
   */
  private notifyAvailabilityChange(serial: string, isAvailable: boolean): void {
    if (this.onAvailabilityChange) {
      try {
        this.onAvailabilityChange(serial, isAvailable);
      } catch (error) {
        console.error(`[DeviceAvailability] Error in availability change handler for ${serial}:`, error);
      }
    }
  }

  /**
   * Get current availability state for a device
   */
  getAvailability(serial: string): boolean {
    const state = this.deviceStates.get(serial);
    // Devices are unavailable until first seen
    return state?.isAvailable ?? false;
  }

  /**
   * Get all device states (for debugging)
   */
  getAllStates(): DeviceAvailabilityState[] {
    return Array.from(this.deviceStates.values());
  }

  /**
   * Force a device to unavailable (for testing or manual override)
   */
  forceUnavailable(serial: string): void {
    const state = this.deviceStates.get(serial);
    if (state && state.isAvailable) {
      console.log(`[DeviceAvailability] Forcing device ${serial} to unavailable`);
      state.isAvailable = false;
      this.notifyAvailabilityChange(serial, false);
    }
  }
}
