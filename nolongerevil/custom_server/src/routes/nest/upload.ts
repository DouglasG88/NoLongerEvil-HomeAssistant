/**
 * /nest/upload Route Handler
 * Device upload endpoint for logs and diagnostics
 */

import { IncomingMessage, ServerResponse } from 'http';
import { writeFileSync, mkdirSync, existsSync } from 'fs';
import { join } from 'path';
import { environment } from '../../config/environment';

/**
 * Handle POST /nest/upload
 * Saves gzipped log files from Nest devices
 */
export function handleUpload(req: IncomingMessage, res: ServerResponse): void {
  // Read raw body as buffer
  const chunks: Buffer[] = [];
  
  req.on('data', (chunk) => {
    chunks.push(chunk);
  });
  
  req.on('end', () => {
    const buffer = Buffer.concat(chunks);
    
    // Ensure debug logs directory exists
    if (!existsSync(environment.DEBUG_LOGS_DIR)) {
      mkdirSync(environment.DEBUG_LOGS_DIR, { recursive: true });
      console.log(`[Upload] Created logs directory: ${environment.DEBUG_LOGS_DIR}`);
    }
    
    // Extract metadata from headers
    const sequenceNumber = req.headers['x-nl-log-file-sequence-number'] as string;
    const timestamp = req.headers['x-nl-log-file-timestamp'] as string;
    const logType = req.headers['x-nl-log-file-type'] as string;
    const version = req.headers['x-nl-log-file-version'] as string;
    const macAddress = req.headers['x-nl-mac-address'] as string;
    
    // Generate filename: seq_timestamp_type_mac.gz
    const filename = `${sequenceNumber || 'unknown'}_${timestamp || 'unknown'}_${logType || 'unknown'}_v${version || '0'}_${macAddress || 'unknown'}.gz`;
    const filepath = join(environment.DEBUG_LOGS_DIR, filename);
    
    try {
      writeFileSync(filepath, buffer);
      console.log(`[Upload] Saved log file: ${filename} (${buffer.length} bytes)`);
    } catch (error) {
      console.error(`[Upload] Failed to save log file: ${filename}`, error);
    }
    
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ status: 'ok' }));
  });
  
  req.on('error', (error) => {
    console.error('[Upload] Error reading request:', error);
    res.writeHead(500, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ status: 'error', message: 'Upload failed' }));
  });
}
