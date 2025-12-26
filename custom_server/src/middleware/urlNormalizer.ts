/**
 * URL Normalization Middleware
 *
 * Provides backward compatibility with legacy Nest device firmware that may request
 * endpoints without the /nest prefix (e.g., /entry instead of /nest/entry).
 *
 */

import { IncomingMessage } from 'http';
import * as url from 'url';

/**
 * Normalize request URL to use /nest prefix for device endpoints
 * Maintains backward compatibility with devices using legacy URL patterns
 */
export function normalizeUrl(req: IncomingMessage): void {
  if (!req.url) {
    return;
  }

  const parsedUrl = url.parse(req.url, true);
  const pathname = parsedUrl.pathname || '';

  if (pathname.startsWith('/nest') || pathname.startsWith('/api') || pathname.startsWith('/command') || pathname.startsWith('/status')) {
    return;
  }

  let normalizedPath: string | null = null;

  if (pathname === '/entry' || pathname.includes('/entry')) {
    normalizedPath = '/nest/entry';
  } else if (pathname === '/passphrase' || pathname.includes('/passphrase')) {
    normalizedPath = '/nest/passphrase';
  } else if (pathname === '/ping' || pathname.includes('/ping')) {
    normalizedPath = '/nest/ping';
  } else if (pathname === '/upload' || pathname.includes('/upload')) {
    normalizedPath = '/nest/upload';
  } else if (pathname.includes('/pro_info') || pathname.includes('/pro-info')) {
    normalizedPath = pathname.replace(/^\/(pro[_-]info)/, '/nest/$1');
  } else if (pathname.startsWith('/weather/v1') || pathname.includes('/weather')) {
    normalizedPath = pathname.startsWith('/weather/v1')
      ? pathname.replace(/^\/weather/, '/nest/weather')
      : '/nest/weather/v1';
  } else if (pathname === '/czfe' || pathname.includes('/czfe')) {
    normalizedPath = '/nest/transport';
  } else if (pathname.includes('/transport') || pathname.includes('/put')) {
    if (pathname.includes('/put')) {
      normalizedPath = '/nest/transport/put';
    } else if (pathname.match(/\/transport\/device\/[A-Z0-9]+/)) {
      normalizedPath = pathname.replace(/^\/transport/, '/nest/transport');
    } else {
      normalizedPath = '/nest/transport';
    }
  }

  if (normalizedPath) {
    const queryString = parsedUrl.search || '';
    req.url = normalizedPath + queryString;
  }
}
