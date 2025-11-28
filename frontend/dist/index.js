"use strict";
/**
 * NoLongerEvil Frontend - Ingress Web UI
 *
 * Provides a simple web interface for device management
 * Handles device registration directly via SQLite database
 * Initializes MQTT integration on startup
 */
var __createBinding = (this && this.__createBinding) || (Object.create ? (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    var desc = Object.getOwnPropertyDescriptor(m, k);
    if (!desc || ("get" in desc ? !m.__esModule : desc.writable || desc.configurable)) {
      desc = { enumerable: true, get: function() { return m[k]; } };
    }
    Object.defineProperty(o, k2, desc);
}) : (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    o[k2] = m[k];
}));
var __setModuleDefault = (this && this.__setModuleDefault) || (Object.create ? (function(o, v) {
    Object.defineProperty(o, "default", { enumerable: true, value: v });
}) : function(o, v) {
    o["default"] = v;
});
var __importStar = (this && this.__importStar) || (function () {
    var ownKeys = function(o) {
        ownKeys = Object.getOwnPropertyNames || function (o) {
            var ar = [];
            for (var k in o) if (Object.prototype.hasOwnProperty.call(o, k)) ar[ar.length] = k;
            return ar;
        };
        return ownKeys(o);
    };
    return function (mod) {
        if (mod && mod.__esModule) return mod;
        var result = {};
        if (mod != null) for (var k = ownKeys(mod), i = 0; i < k.length; i++) if (k[i] !== "default") __createBinding(result, mod, k[i]);
        __setModuleDefault(result, mod);
        return result;
    };
})();
Object.defineProperty(exports, "__esModule", { value: true });
const http = __importStar(require("http"));
const mqtt_init_1 = require("./mqtt-init");
const device_registration_1 = require("./device-registration");
const server_1 = require("./server");
const PORT = parseInt(process.env.INGRESS_PORT || '8082');
async function start() {
    console.log('[Frontend] Starting NoLongerEvil Web UI...');
    // Initialize MQTT integration first
    try {
        await (0, mqtt_init_1.initializeMqtt)();
    }
    catch (error) {
        console.error('[Frontend] MQTT initialization failed:', error);
        process.exit(1);
    }
    // Ensure homeassistant user exists
    try {
        await (0, device_registration_1.ensureHomeAssistantUser)();
    }
    catch (error) {
        console.error('[Frontend] Failed to create homeassistant user:', error);
        process.exit(1);
    }
    // Start web server
    const server = http.createServer(server_1.handleRequest);
    // Listen on all interfaces for Ingress
    server.listen(PORT, '0.0.0.0', () => {
        console.log(`[Frontend] Web UI listening on port ${PORT}`);
        console.log('[Frontend] Ready for Ingress connections');
    });
}
start().catch(error => {
    console.error('[Frontend] Failed to start:', error);
    process.exit(1);
});
