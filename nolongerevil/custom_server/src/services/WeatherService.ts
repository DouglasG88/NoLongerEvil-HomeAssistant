/**
 * WeatherService
 *
 * Handles weather data caching and proxying to weather.nest.com
 * Implements 10-minute cache in device state manager to reduce external API calls
 */

import * as https from 'https';
import type { WeatherData, StateWeatherCache } from '../lib/types';
import { environment } from '../config/environment';
import { AbstractDeviceStateManager } from './AbstractDeviceStateManager';

export class WeatherService {
  private deviceStateManager: AbstractDeviceStateManager;

  constructor(deviceStateManager: AbstractDeviceStateManager) {
    this.deviceStateManager = deviceStateManager;
  }

  /**
   * Get weather data for a location
   * Uses cache if available and not expired, otherwise fetches from weather.nest.com
   *
   * @param query - Format: "postalCode,country" or "ipv4"/"ipv6"
   * @returns Weather data or null on error
   */
  async getWeather(query: string): Promise<WeatherData | null> {
    const { postalCode, country, isIpQuery } = this.parseQuery(query);

    if (!isIpQuery && postalCode && country) {
      const cached = await this.getCachedWeather(postalCode, country);
      if (cached) {
        console.log(`[WeatherService] Cache hit for ${postalCode}, ${country}`);
        return cached.data;
      }
    }

    console.log(`[WeatherService] Fetching weather for query: ${query}`);
    const weatherData = await this.fetchWeatherFromNest(query);

    if (!weatherData) {
      return null;
    }

    if (!isIpQuery && postalCode && country) {
      await this.cacheWeather(postalCode, country, weatherData);
    }

    return weatherData;
  }

  /**
   * Parse weather query string
   */
  private parseQuery(query: string): {
    postalCode: string | null;
    country: string | null;
    isIpQuery: boolean;
  } {
    if (query.startsWith('ipv4') || query.startsWith('ipv6')) {
      return { postalCode: null, country: null, isIpQuery: true };
    }

    const parts = query.split(',');
    if (parts.length === 2) {
      return {
        postalCode: parts[0].trim(),
        country: parts[1].trim(),
        isIpQuery: false,
      };
    }

    return { postalCode: null, country: null, isIpQuery: false };
  }

  /**
   * Get cached weather from device state manager
   */
  private async getCachedWeather(
    postalCode: string,
    country: string
  ): Promise<StateWeatherCache | null> {
    const cached = await this.deviceStateManager.getWeather(postalCode, country);

    if (!cached) {
      return null;
    }

    const age = Date.now() - cached.fetchedAt;
    if (age > environment.WEATHER_CACHE_TTL_MS) {
      console.log(`[WeatherService] Cache expired for ${postalCode}, ${country} (age: ${age}ms)`);
      return null;
    }

    return cached;
  }

  /**
   * Cache weather data in device state manager
   */
  private async cacheWeather(
    postalCode: string,
    country: string,
    data: WeatherData
  ): Promise<void> {
    await this.deviceStateManager.upsertWeather(postalCode, country, Date.now(), data);
  }

  /**
   * Fetch weather from weather.nest.com
   */
  private fetchWeatherFromNest(query: string): Promise<WeatherData | null> {
    return new Promise((resolve) => {
      const url = `https://weather.nest.com/weather/v1?query=${encodeURIComponent(query)}`;

      https.get(url, { rejectUnauthorized: false }, (res) => {
        let data = '';

        res.on('data', (chunk) => {
          data += chunk;
        });

        res.on('end', () => {
          if (res.statusCode !== 200) {
            console.error(`[WeatherService] Weather API returned ${res.statusCode}`);
            resolve(null);
            return;
          }

          try {
            const weatherData = JSON.parse(data);
            resolve(weatherData);
          } catch (error) {
            console.error('[WeatherService] Failed to parse weather response:', error);
            resolve(null);
          }
        });
      }).on('error', (error) => {
        console.error('[WeatherService] Weather API request failed:', error);
        resolve(null);
      });
    });
  }
}
