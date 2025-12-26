/**
 * File Logger Utility
 *
 * When DEBUG_LOGGING=true, logs are written to both console and file.
 * Log files are stored in the configured debug logs directory.
 */

import fs from 'fs';
import path from 'path';
import { environment } from '../config/environment';

const MAX_LOG_SIZE = 10 * 1024 * 1024; // 10MB
const MAX_LOG_FILES = 5;

if (environment.DEBUG_LOGGING) {
  if (!fs.existsSync(environment.DEBUG_LOGS_DIR)) {
    fs.mkdirSync(environment.DEBUG_LOGS_DIR, { recursive: true });
  }
}

/**
 * Get current log file path
 */
function getCurrentLogFile(): string {
  const date = new Date().toISOString().split('T')[0]; // YYYY-MM-DD
  return path.join(environment.DEBUG_LOGS_DIR, `server-${date}.log`);
}

/**
 * Rotate log file if it exceeds max size
 */
function rotateLogIfNeeded(logFile: string): void {
  try {
    if (fs.existsSync(logFile)) {
      const stats = fs.statSync(logFile);
      if (stats.size > MAX_LOG_SIZE) {
        const timestamp = new Date().toISOString().replace(/[:.]/g, '-');
        const rotatedFile = logFile.replace('.log', `-${timestamp}.log`);
        fs.renameSync(logFile, rotatedFile);

        cleanupOldLogs();
      }
    }
  } catch (error) {
  }
}

/**
 * Clean up old log files, keeping only MAX_LOG_FILES
 */
function cleanupOldLogs(): void {
  try {
    const files = fs.readdirSync(environment.DEBUG_LOGS_DIR)
      .filter(f => f.startsWith('server-') && f.endsWith('.log'))
      .map(f => ({
        name: f,
        path: path.join(environment.DEBUG_LOGS_DIR, f),
        mtime: fs.statSync(path.join(environment.DEBUG_LOGS_DIR, f)).mtime.getTime()
      }))
      .sort((a, b) => b.mtime - a.mtime);

    files.slice(MAX_LOG_FILES).forEach(file => {
      try {
        fs.unlinkSync(file.path);
      } catch (error) {
      }
    });
  } catch (error) {
  }
}

/**
 * Write log entry to file
 */
function writeToFile(message: string): void {
  if (!environment.DEBUG_LOGGING) {
    return;
  }

  try {
    const logFile = getCurrentLogFile();
    rotateLogIfNeeded(logFile);

    const timestamp = new Date().toISOString();
    const logEntry = `[${timestamp}] ${message}\n`;

    fs.appendFileSync(logFile, logEntry, 'utf8');
  } catch (error) {
  }
}

/**
 * Enhanced console.log that also writes to file when DEBUG_LOGGING is enabled
 */
const originalConsoleLog = console.log;
const originalConsoleError = console.error;
const originalConsoleWarn = console.warn;

console.log = function(...args: any[]) {
  const message = args.map(arg =>
    typeof arg === 'object' ? JSON.stringify(arg, null, 2) : String(arg)
  ).join(' ');

  writeToFile(`[LOG] ${message}`);
  originalConsoleLog.apply(console, args);
};

console.error = function(...args: any[]) {
  const message = args.map(arg =>
    typeof arg === 'object' ? JSON.stringify(arg, null, 2) : String(arg)
  ).join(' ');

  writeToFile(`[ERROR] ${message}`);
  originalConsoleError.apply(console, args);
};

console.warn = function(...args: any[]) {
  const message = args.map(arg =>
    typeof arg === 'object' ? JSON.stringify(arg, null, 2) : String(arg)
  ).join(' ');

  writeToFile(`[WARN] ${message}`);
  originalConsoleWarn.apply(console, args);
};

/**
 * Initialize file logging
 */
export function initializeFileLogging(): void {
  if (environment.DEBUG_LOGGING) {
    console.log(`[Logger] File logging enabled - writing to ${environment.DEBUG_LOGS_DIR}`);
    console.log(`[Logger] Log rotation: ${MAX_LOG_SIZE / 1024 / 1024}MB per file, keeping ${MAX_LOG_FILES} files`);
  }
}

/**
 * Get path to current log file (for debugging)
 */
export function getLogFilePath(): string | null {
  if (!environment.DEBUG_LOGGING) {
    return null;
  }
  return getCurrentLogFile();
}
