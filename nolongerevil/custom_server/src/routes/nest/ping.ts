/**
 * /nest/ping Route Handler
 * Health check endpoint
 */

import { IncomingMessage, ServerResponse } from 'http';

/**
 * Handle GET /nest/ping
 * Simple health check endpoint
 */
export function handlePing(_req: IncomingMessage, res: ServerResponse): void {
  res.writeHead(200, { 'Content-Type': 'application/json' });
  res.end(JSON.stringify({ status: 'ok', timestamp: Date.now() }));
}
