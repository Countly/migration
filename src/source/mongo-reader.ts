import { MongoClient, ReadPreference, type Collection, type Db, type Document, type Filter } from "mongodb";
import type { Logger } from "pino";
import { type Cursor, cdToEpoch } from '../types/cursor.ts';
import type { SourceDocument } from '../transform/normalize.ts';

export interface MongoReaderConfig {
  uri: string;
  database: string;
  readPreference: string;
  readConcern: string;
  retryReads: boolean;
  appName: string;
  batchRowsTarget: number;
  cursorBatchSize: number;
  maxTimeMs: number;
}

export interface PageResult {
  docs: SourceDocument[];
  lastCursor: Cursor | null;
  fetchMs: number;
}

const PROJECTION = {
  a: 1,
  e: 1,
  n: 1,
  uid: 1,
  uid_canon: 1,
  did: 1,
  lsid: 1,
  _id: 1,
  ts: 1,
  up: 1,
  custom: 1,
  cmp: 1,
  sg: 1,
  c: 1,
  s: 1,
  dur: 1,
  lu: 1,
  cd: 1,
  migrated: 1,
} as const;

export class MongoReader {
  private readonly config: MongoReaderConfig;
  private readonly logger: Logger;
  private client: MongoClient | null = null;
  private db: Db | null = null;
  private collection: Collection<Document> | null = null;
  private currentCollectionName: string | null = null;
  private connected = false;

  constructor(config: MongoReaderConfig, logger: Logger) {
    this.config = config;
    this.logger = logger.child({ component: "MongoReader" });
  }

  async connect(): Promise<void> {
    const { uri, database, readPreference, readConcern, retryReads, appName } = this.config;

    this.logger.info({ database, readPreference, readConcern }, "Connecting to MongoDB");

    this.client = new MongoClient(uri, {
      readPreference: ReadPreference.fromString(readPreference),
      retryReads,
      appName,
      readConcern: { level: readConcern as "local" | "majority" | "linearizable" | "available" | "snapshot" },
      serverSelectionTimeoutMS: 30_000,
      connectTimeoutMS: 10_000,
      socketTimeoutMS: this.config.maxTimeMs + 30_000,
    });

    await this.client.connect();
    this.db = this.client.db(database);
    this.connected = true;

    this.logger.info("Connected to MongoDB");
  }

  /**
   * Check if a collection has the required { cd: 1, _id: 1 } compound index.
   */
  async hasRequiredIndex(collectionName: string): Promise<boolean> {
    if (!this.connected || !this.db) {
      throw new Error("MongoReader is not connected. Call connect() first.");
    }
    const coll = this.db.collection(collectionName);
    const indexes = await coll.indexes();
    return indexes.some(idx => {
      const key = idx.key as Record<string, unknown>;
      return key['cd'] !== undefined && key['_id'] !== undefined;
    });
  }

  /**
   * Start index creation for a collection. This may take a long time for
   * large collections. The returned promise resolves when the index is built.
   */
  async startIndexCreation(collectionName: string): Promise<void> {
    if (!this.connected || !this.db) {
      throw new Error("MongoReader is not connected. Call connect() first.");
    }
    const coll = this.db.collection(collectionName);
    this.logger.info({ collection: collectionName }, "Starting index creation { cd: 1, _id: 1 }");
    await coll.createIndex({ cd: 1, _id: 1 });
    this.logger.info({ collection: collectionName }, "Index creation completed");
  }

  /**
   * Switch to a different collection within the same database.
   * Caller is responsible for ensuring the required index exists.
   */
  async switchCollection(collectionName: string): Promise<void> {
    if (!this.connected || !this.db) {
      throw new Error("MongoReader is not connected. Call connect() first.");
    }

    this.collection = this.db.collection(collectionName);
    this.currentCollectionName = collectionName;
    this.logger.info({ collection: collectionName }, "Switched to collection");
  }

  /**
   * Returns the underlying Db instance for use with collection discovery.
   */
  getDatabase(): Db {
    if (!this.db) {
      throw new Error("MongoReader is not connected. Call connect() first.");
    }
    return this.db;
  }

  async getUpperBound(): Promise<Cursor | null> {
    this.ensureConnected();

    const result = await this.collection!
      .find({})
      .sort({ cd: -1, _id: -1 })
      .hint({ cd: 1, _id: 1 })
      .limit(1)
      .project({ cd: 1, _id: 1 })
      .maxTimeMS(this.config.maxTimeMs)
      .toArray();

    if (result.length === 0) {
      this.logger.info({ collection: this.currentCollectionName }, "Collection is empty");
      return null;
    }

    const upperBound: Cursor = { cd: cdToEpoch(result[0].cd), id: String(result[0]._id) };
    this.logger.info({ upperBound }, "Determined upper bound cursor");
    return upperBound;
  }

  async getEstimatedCount(): Promise<number> {
    this.ensureConnected();

    const count = await this.collection!.estimatedDocumentCount();
    this.logger.info({ estimatedCount: count }, "Estimated document count");
    return count;
  }

  async readPage(lastCursor: Cursor | null, upperBound: Cursor, limit?: number): Promise<PageResult> {
    this.ensureConnected();

    const { cursorBatchSize, maxTimeMs } = this.config;
    const pageLimit = limit ?? this.config.batchRowsTarget;

    const ucd = new Date(upperBound.cd);
    const upperFilter = {
      $or: [
        { cd: { $lt: ucd } },
        { cd: ucd, _id: { $lte: upperBound.id } }
      ]
    };

    let filter: Filter<Document>;
    if (lastCursor === null) {
      filter = upperFilter as Filter<Document>;
    } else {
      const lcd = new Date(lastCursor.cd);
      const lowerFilter = {
        $or: [
          { cd: { $gt: lcd } },
          { cd: lcd, _id: { $gt: lastCursor.id } }
        ]
      };
      filter = { $and: [lowerFilter, upperFilter] } as Filter<Document>;
    }

    const startMs = performance.now();

    const docs = await this.collection!
      .find(filter)
      .sort({ cd: 1, _id: 1 })
      .hint({ cd: 1, _id: 1 })
      .limit(pageLimit)
      .batchSize(cursorBatchSize)
      .project(PROJECTION)
      .maxTimeMS(maxTimeMs)
      .toArray();

    const fetchMs = Math.round(performance.now() - startMs);

    const lastDoc = docs[docs.length - 1];
    const lastCursorResult: Cursor | null = docs.length > 0
      ? { cd: cdToEpoch(lastDoc.cd), id: String(lastDoc._id) }
      : null;

    this.logger.debug(
      { docsRead: docs.length, lastCursor: lastCursorResult, fetchMs },
      "Page read complete",
    );

    return {
      docs: docs as SourceDocument[],
      lastCursor: lastCursorResult,
      fetchMs,
    };
  }

  isConnected(): boolean {
    return this.connected;
  }

  async close(): Promise<void> {
    if (this.client) {
      this.logger.info("Closing MongoDB connection");
      await this.client.close();
      this.client = null;
      this.db = null;
      this.collection = null;
      this.currentCollectionName = null;
      this.connected = false;
      this.logger.info("MongoDB connection closed");
    }
  }

  private ensureConnected(): void {
    if (!this.connected || !this.collection) {
      throw new Error("MongoReader is not connected. Call connect() first.");
    }
  }
}
