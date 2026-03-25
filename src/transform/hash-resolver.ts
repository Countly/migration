/**
 * Resolves drill_events{sha1} collection names to (appId, eventName) pairs.
 *
 * Connects to the main Countly database, reads all apps and their events,
 * and builds a reverse-lookup map:  SHA1(eventName + appId) → { a, e }.
 *
 * This allows the migration to fill in missing `a` and `e` fields for
 * documents in hashed collections where those fields were implicit.
 */

import { createHash } from "node:crypto";
import { MongoClient, type Db } from "mongodb";
import type { Logger } from "pino";

// ─────────────────────────────────────────────────────────────────────────────
// Public types
// ─────────────────────────────────────────────────────────────────────────────

/** Default `a` (appId) and `e` (eventName) values derived from a collection hash. */
export interface CollectionDefaults {
  a: string;
  e: string;
}

export interface HashResolverConfig {
  uri: string;
  countlyDb: string;
}

// ─────────────────────────────────────────────────────────────────────────────
// Known Countly internal event names (from drill_old2new.js)
// ─────────────────────────────────────────────────────────────────────────────

const KNOWN_CLY_EVENTS: readonly string[] = [
  "[CLY]_view",
  "[CLY]_session",
  "[CLY]_crash",
  "[CLY]_push_action",
  "[CLY]_push_sent",
  "[CLY]_consent",
  "[CLY]_star_rating",
  "[CLY]_nps",
  "[CLY]_survey",
  "[CLY]_action",
  "[CLY]_apm_device",
  "[CLY]_apm_network",
  "[CLY]_orientation",
  "[CLY]_llm_interaction",
  "[CLY]_llm_interaction_feedback",
  "[CLY]_llm_tool_used",
  "[CLY]_llm_tool_usage_parameter",
  "[CLY]_journey_engine_start",
  "[CLY]_journey_engine_end",
  "[CLY]_content_shown",
  "[CLY]_content_interacted",
  "[CLY]_AB_E",
  "[CLY]_Event",
  "[CLY]_apm_",
  "[CLY]_cohorts",
  "[CLY]_consolidated",
  "[CLY]_consolidated_",
  "[CLY]_custom",
  "[CLY]_custom_event",
  "[CLY]_event_name",
  "[CLY]_exit",
  "[CLY]_group",
  "[CLY]_group_",
  "[CLY]_group_page_views",
  "[CLY]_journey_completed",
  "[CLY]_journey_engine",
  "[CLY]_journey_entered",
  "[CLY]_nps_shown",
  "[CLY]_property_update",
  "[CLY]_purchase",
  "[CLY]_push",
  "[CLY]_push_",
  "[CLY]_push_consent",
  "[CLY]_push_open",
  "[CLY]_push_sent_507f1f77bcf86cd799439012_no",
  "[CLY]_session0",
  "[CLY]_session2",
  "[CLY]_session_begin",
  "[CLY]_session_update",
  "[CLY]_sessions",
  "[CLY]_special",
  "[CLY]_star",
  "[CLY]_survery",
  "[CLY]_survey_shown",
  "[CLY]_true",
  "[CLY]_view_begin",
  "[CLY]_view_update",
  "[CLY]_views",
];

// ─────────────────────────────────────────────────────────────────────────────
// HashResolver
// ─────────────────────────────────────────────────────────────────────────────

export class HashResolver {
  private readonly config: HashResolverConfig;
  private readonly logger: Logger;
  private client: MongoClient | null = null;
  private hashMap: Map<string, CollectionDefaults> = new Map();

  constructor(config: HashResolverConfig, logger: Logger) {
    this.config = config;
    this.logger = logger.child({ component: "HashResolver" });
  }

  /**
   * Connect to the main Countly database, read all apps and their events,
   * and build the SHA1 hash lookup map.
   */
  async build(): Promise<void> {
    this.client = new MongoClient(this.config.uri, {
      serverSelectionTimeoutMS: 30_000,
      connectTimeoutMS: 10_000,
    });

    await this.client.connect();
    const db: Db = this.client.db(this.config.countlyDb);

    // 1. Read all apps
    const apps = await db.collection("apps").find({}).project({ _id: 1 }).toArray();

    if (apps.length === 0) {
      this.logger.warn("No apps found in countly.apps — hash map will be empty");
      return;
    }

    this.logger.info({ appCount: apps.length }, "Read apps from countly database");

    // 2. For each app, read custom events and build hashes
    for (const app of apps) {
      const appId = String(app._id);

      // Read custom events for this app
      const eventsDoc = await db.collection("events").findOne({ _id: app._id });
      const customEvents: string[] = Array.isArray(eventsDoc?.list) ? eventsDoc.list : [];

      // Combine known + custom events (deduplicate via Set)
      const allEvents = new Set<string>([...KNOWN_CLY_EVENTS, ...customEvents]);

      for (const eventName of allEvents) {
        const hash = createHash("sha1").update(eventName + appId).digest("hex");
        this.hashMap.set(hash, { a: appId, e: eventName });
      }
    }

    this.logger.info(
      { hashEntries: this.hashMap.size, appCount: apps.length },
      "Hash resolver map built",
    );
  }

  /** Resolve a raw 40-char SHA1 hash to (appId, eventName) or null. */
  resolve(hash: string): CollectionDefaults | null {
    return this.hashMap.get(hash) ?? null;
  }

  /**
   * Given a full collection name and the collection prefix, extract the
   * hash suffix and resolve it.
   *
   * Returns null if:
   * - The collection is the base collection (no hash suffix)
   * - The hash suffix is not 40 characters (not a valid SHA1)
   * - The hash doesn't match any known (app, event) pair
   */
  resolveCollectionName(
    collectionName: string,
    prefix: string,
  ): CollectionDefaults | null {
    if (collectionName === prefix) return null;

    const hash = collectionName.slice(prefix.length);
    if (hash.length !== 40) return null;

    return this.hashMap.get(hash) ?? null;
  }

  /** Number of entries in the hash map. */
  get size(): number {
    return this.hashMap.size;
  }

  /** Close the dedicated MongoDB connection. */
  async close(): Promise<void> {
    if (this.client) {
      await this.client.close();
      this.client = null;
      this.logger.info("Hash resolver MongoDB connection closed");
    }
  }
}
