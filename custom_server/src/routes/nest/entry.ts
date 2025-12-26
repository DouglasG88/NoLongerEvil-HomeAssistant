/**
 * /nest/entry Route Handler
 * Service discovery endpoint - returns URLs for all Nest services
 */

import { IncomingMessage, ServerResponse } from 'http';
import { environment } from '../../config/environment';

/**
 * Handle GET /nest/entry
 * Returns service discovery URLs for device initialization
 */
export function handleEntry(_req: IncomingMessage, res: ServerResponse): void {
  const baseUrl =  environment.API_ORIGIN

  const response = {
    czfe_url: `${baseUrl}/nest/transport`,
    transport_url: `${baseUrl}/nest/transport`,
    direct_transport_url: `${baseUrl}/nest/transport`,
    passphrase_url: `${baseUrl}/nest/passphrase`,
    ping_url: `${baseUrl}/nest/transport`,
    pro_info_url: `${baseUrl}/nest/pro_info`,
    weather_url: `${baseUrl}/nest/weather/v1?query=`,
    upload_url: `${baseUrl}/nest/upload`,
    software_update_url: '',
    server_version: '1.0.0',
    tier_name: 'local',
  };

  res.writeHead(200, { 'Content-Type': 'application/json' });
  res.end(JSON.stringify(response));
}
