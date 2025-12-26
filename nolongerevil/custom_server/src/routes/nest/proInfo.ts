/**
 * /nest/pro_info Route Handler
 * Installer information lookup
 */

import { IncomingMessage, ServerResponse } from 'http';
import * as url from 'url';

/**
 * Handle GET /nest/pro_info/{CODE} or /nest/pro-info/{CODE}
 * Returns installer information for the specified pro code
 */
export function handleProInfo(req: IncomingMessage, res: ServerResponse): void {
  const parsedUrl = url.parse(req.url || '', true);
  const pathname = parsedUrl.pathname || '';

  const match = pathname.match(/\/nest\/pro[-_]info\/([A-Z0-9]+)/i);
  const proId = match ? match[1].toUpperCase() : null;

  if (!proId) {
    res.writeHead(400, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ error: 'Missing entry code' }));
    return;
  }

  const proInfo = {
    id: 1,
    pro_id: proId,
    dba: 'Hack House',
    street_address_1: '7975 N Hayden Rd',
    street_address_2: 'Suite A210',
    locality: 'Scottsdale',
    region: 'Arizona',
    postal_code: '85388',
    email: 'cody@hackhouse.io',
    phone: '(855) 994-1337',
    website: 'https://nolongerevil.com',
    rating: 5.0,
    plain_email_address_for_referrals: 'cody@hackhouse.io',
  };

  const response = {
    [proId]: proInfo
  };

  res.writeHead(200, { 'Content-Type': 'application/json' });
  res.end(JSON.stringify(response));
}
