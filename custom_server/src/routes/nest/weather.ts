/**
 * /nest/weather/v1 Route Handler
 *
 * Proxy to weather.nest.com with caching
 */

import { IncomingMessage, ServerResponse } from 'http';
import { WeatherService } from '../../services/WeatherService';
import * as url from 'url';

/**
 * Handle GET /nest/weather/v1
 * Proxies weather requests to weather.nest.com with caching
 */
export async function handleWeather(
  req: IncomingMessage,
  res: ServerResponse,
  weatherService: WeatherService
): Promise<void> {
  const parsedUrl = url.parse(req.url || '', true);
  const query = parsedUrl.query.query as string;

  if (!query) {
    res.writeHead(400, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ error: 'Missing query parameter' }));
    return;
  }

  console.log(`[Weather] Fetching weather for query: ${query}`);

  const weatherData = await weatherService.getWeather(query);

  if (!weatherData) {
    res.writeHead(502, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ error: 'Weather service unavailable' }));
    return;
  }

  res.writeHead(200, { 'Content-Type': 'application/json' });
  res.end(JSON.stringify(weatherData));
}
