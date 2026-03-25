import type { Db } from "mongodb";
import type { Logger } from "pino";

function escapeRegex(str: string): string {
    return str.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

/**
 * Discover all MongoDB collections whose name starts with the given prefix.
 * Returns a sorted array of collection names (alphabetical, deterministic).
 * Throws if zero collections are found.
 */
export async function discoverCollections(
    db: Db,
    prefix: string,
    logger: Logger,
): Promise<string[]> {
    const pattern = new RegExp(`^${escapeRegex(prefix)}`);
    const collections = await db.listCollections({ name: { $regex: pattern } }).toArray();

    const names = collections.map((c) => c.name).sort();

    // Ensure the base collection (exact prefix match) is always processed first
    const baseIdx = names.indexOf(prefix);
    if (baseIdx > 0) {
        names.splice(baseIdx, 1);
        names.unshift(prefix);
    }

    if (names.length === 0) {
        throw new Error(
            `No collections found matching prefix "${prefix}" in database "${db.databaseName}"`,
        );
    }

    logger.info(
        { prefix, count: names.length, collections: names },
        "Discovered collections for migration",
    );

    return names;
}
