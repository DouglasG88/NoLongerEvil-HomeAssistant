"use strict";
/**
 * Device Registration Module
 *
 * Handles device registration directly via SQLite database
 * Similar to mqtt-init.ts - keeps vendor code clean
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
exports.ensureHomeAssistantUser = ensureHomeAssistantUser;
exports.claimEntryKey = claimEntryKey;
exports.registerDeviceToUser = registerDeviceToUser;
exports.getDevicesForUser = getDevicesForUser;
const sqlite3 = __importStar(require("sqlite3"));
const sqlite_1 = require("sqlite");
const DB_PATH = process.env.SQLITE3_DB_PATH || '/data/database.sqlite';
/**
 * Open database connection
 */
async function openDb() {
    return (0, sqlite_1.open)({
        filename: DB_PATH,
        driver: sqlite3.Database,
    });
}
/**
 * Create the 'homeassistant' user if it doesn't exist
 */
async function ensureHomeAssistantUser() {
    const db = await openDb();
    try {
        const userId = 'homeassistant';
        const email = 'homeassistant@local';
        // Check if user exists
        const existing = await db.get('SELECT clerkId FROM users WHERE clerkId = ?', [userId]);
        if (existing) {
            console.log('[DeviceReg] User "homeassistant" already exists');
        }
        else {
            const nowMs = Date.now();
            await db.run('INSERT INTO users (clerkId, email, createdAt) VALUES (?, ?, ?)', [userId, email, nowMs]);
            console.log('[DeviceReg] ✅ Created user "homeassistant"');
        }
    }
    finally {
        await db.close();
    }
}
/**
 * Claim an entry key and return the device serial
 */
async function claimEntryKey(code, userId) {
    const db = await openDb();
    try {
        const nowMs = Date.now();
        // Get entry key
        const row = await db.get('SELECT code, serial, expiresAt, claimedBy FROM entryKeys WHERE code = ?', [code]);
        if (!row) {
            console.warn(`[DeviceReg] Entry key not found: ${code}`);
            return null;
        }
        // Check if expired
        if (row.expiresAt < nowMs) {
            console.warn(`[DeviceReg] Entry key expired: ${code}`);
            return null;
        }
        // Check if already claimed
        if (row.claimedBy) {
            console.warn(`[DeviceReg] Entry key already claimed: ${code}`);
            return null;
        }
        // Mark as claimed
        await db.run('UPDATE entryKeys SET claimedBy = ?, claimedAt = ? WHERE code = ?', [userId, nowMs, code]);
        console.log(`[DeviceReg] ✅ Claimed entry key ${code} for device ${row.serial}`);
        return row.serial;
    }
    finally {
        await db.close();
    }
}
/**
 * Register device to user (create ownership record)
 */
async function registerDeviceToUser(userId, serial) {
    const db = await openDb();
    try {
        const nowMs = Date.now();
        // Check if already registered
        const existing = await db.get('SELECT userId FROM deviceOwners WHERE serial = ?', [serial]);
        if (existing) {
            console.warn(`[DeviceReg] Device ${serial} already registered to ${existing.userId}`);
            return;
        }
        // Insert ownership record
        await db.run('INSERT INTO deviceOwners (userId, serial, createdAt) VALUES (?, ?, ?)', [userId, serial, nowMs]);
        console.log(`[DeviceReg] ✅ Registered device ${serial} to user ${userId}`);
    }
    finally {
        await db.close();
    }
}
/**
 * Get list of device serials owned by a user
 * Note: Full device state is stored in-memory by the server, not in the DB
 * This only returns the ownership records (serial numbers)
 */
async function getDevicesForUser(userId) {
    const db = await openDb();
    try {
        const devices = await db.all(`SELECT serial, createdAt 
       FROM deviceOwners
       WHERE userId = ?
       ORDER BY createdAt DESC`, [userId]);
        return devices;
    }
    finally {
        await db.close();
    }
}
