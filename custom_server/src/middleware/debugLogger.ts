/**
 * Debug Logger Middleware
 *
 * Optional request/response logging for troubleshooting and development.
 * Activated via DEBUG_LOGGING environment variable.
 *
 * Logs:
 * - Request: method, URL, headers, body
 * - Response: status code, headers, body
 * - Each request/response pair saved to individual file in /data/debug-logs/
 */

import { IncomingMessage, ServerResponse } from 'http';
import { mkdirSync, writeFileSync, existsSync } from 'fs';
import { join } from 'path';
import { environment } from '../config/environment';

/**
 * Initialize debug logs directory - clean old request JSON files
 * Call this once when the server starts
 * Note: Keeps server-*.log files (managed by logger.ts rotation)
 */
export function initDebugLogsDir(): void {
  if (!environment.DEBUG_LOGGING) {
    return;
  }

  try {
    // Create directory if it doesn't exist
    if (!existsSync(environment.DEBUG_LOGS_DIR)) {
      mkdirSync(environment.DEBUG_LOGS_DIR, { recursive: true });
      console.log(`[Debug Logger] Created logs directory: ${environment.DEBUG_LOGS_DIR}`);
      return;
    }

    // Clean up old request JSON files (keep server-*.log files)
    const fs = require('fs');
    const files = fs.readdirSync(environment.DEBUG_LOGS_DIR);
    let removedCount = 0;
    
    files.forEach((file: string) => {
      // Only remove JSON files (request logs), keep .log files (general logs)
      if (file.endsWith('.json')) {
        try {
          fs.unlinkSync(join(environment.DEBUG_LOGS_DIR, file));
          removedCount++;
        } catch (error) {
          console.error(`[Debug Logger] Failed to remove ${file}:`, error);
        }
      }
    });

    if (removedCount > 0) {
      console.log(`[Debug Logger] Cleaned ${removedCount} old request log(s) from: ${environment.DEBUG_LOGS_DIR}`);
    } else {
      console.log(`[Debug Logger] Logs directory ready: ${environment.DEBUG_LOGS_DIR}`);
    }
  } catch (error) {
    console.error('[Debug Logger] Failed to initialize logs directory:', error);
  }
}

/**
 * Write log entry to individual file
 */
function writeLogFile(logEntry: any, requestId: string): void {
  if (!environment.DEBUG_LOGGING) {
    return;
  }

  try {
    const filename = `${requestId}.json`;
    const filepath = join(environment.DEBUG_LOGS_DIR, filename);
    writeFileSync(filepath, JSON.stringify(sortObjectKeys(logEntry), null, 2));
  } catch (error) {
    console.error('[Debug Logger] Failed to write log file:', error);
  }
}

/**
 * Generate request ID from timestamp and route
 * Format: YYYYMMDD-HHmmss-SSS_route
 */
function generateRequestId(url: string): string {
  const now = new Date();
  const timestamp = now.toISOString()
    .replace(/[:.]/g, '-')
    .replace('T', '_')
    .slice(0, -1); // Remove trailing 'Z'
  
  // Clean up URL for filename (remove query params, leading slash, replace slashes with dashes)
  const route = (url || '/')
    .split('?')[0]
    .replace(/^\//, '')
    .replace(/\//g, '-')
    .replace(/[^a-zA-Z0-9-_]/g, '_') || 'root';

  return `${timestamp}_${route}`;
}

interface LogEntry {
  timestamp: string;
  requestId: string;
  request: {
    method: string;
    url: string;
    headers: Record<string, any>;
    body?: any;
  };
  response?: {
    statusCode: number;
    headers: Record<string, any>;
    body?: any;
  };
}

/**
 * Log request details to console
 */
export function logRequest(req: IncomingMessage, body?: any): void {
  const timestamp = new Date().toISOString();
  const requestId = generateRequestId(req.url || '/');
  
  const logEntry: LogEntry = {
    timestamp,
    requestId,
    request: {
      method: req.method || 'UNKNOWN',
      url: req.url || '/',
      headers: req.headers,
      body: body || undefined,
    },
  };

  console.log('\n--- REQUEST ---');
  const logMessage = JSON.stringify(sortObjectKeys(logEntry), null, 2);
  console.log(logMessage);
  
  // Store request data for matching with response
  (req as any).__requestId = requestId;
  (req as any).__requestLogEntry = logEntry;
}

/**
 * Intercept response to log response details
 * Returns a wrapper function that should be called with response body
 */
export function createResponseLogger(
  req: IncomingMessage,
  res: ServerResponse
): (body?: any) => void {
  const originalWrite = res.write.bind(res);
  const originalEnd = res.end.bind(res);
  let responseBody = '';

  res.write = function (chunk: any, ...args: any[]): boolean {
    if (chunk) {
      responseBody += chunk.toString();
    }
    return originalWrite(chunk, ...args);
  };

  res.end = function (chunk?: any, ...args: any[]): any {
    if (chunk) {
      responseBody += chunk.toString();
    }

    const timestamp = new Date().toISOString();
    const requestId = (req as any).__requestId || generateRequestId(req.url || '/');
    const savedRequestLogEntry = (req as any).__requestLogEntry;
    
    const logEntry = {
      timestamp,
      requestId,
      request: savedRequestLogEntry ? savedRequestLogEntry.request : {
        method: req.method,
        url: req.url,
      },
      response: {
        statusCode: res.statusCode,
        headers: res.getHeaders(),
        body: tryParseJSON(responseBody),
      },
    };

    console.log('\n--- RESPONSE ---');
    const logMessage = JSON.stringify(sortObjectKeys(logEntry), null, 2);
    console.log(logMessage);
    console.log('---\n');
    
    // Write combined request/response to file
    writeLogFile(logEntry, requestId);

    return originalEnd(chunk, ...args);
  };

  return () => {};
}

/**
 * Recursively sort object keys alphabetically
 */
function sortObjectKeys(obj: any): any {
  if (obj === null || typeof obj !== 'object') {
    return obj;
  }

  if (Array.isArray(obj)) {
    return obj.map(sortObjectKeys);
  }

  const sorted: Record<string, any> = {};
  Object.keys(obj)
    .sort()
    .forEach((key) => {
      sorted[key] = sortObjectKeys(obj[key]);
    });

  return sorted;
}

/**
 * Try to parse response body as JSON, fallback to raw string
 */
function tryParseJSON(body: string): any {
  if (!body) {
    return undefined;
  }

  try {
    const parsed = JSON.parse(body);
    return sortObjectKeys(parsed);
  } catch {
    return body.length > 500 ? body.substring(0, 500) + '...' : body;
  }
}
