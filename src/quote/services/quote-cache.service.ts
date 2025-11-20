import { Logger } from '@nestjs/common';
import { DnseQuote } from '../schemas/dnse-quote.schema';
import { Cron, CronExpression } from '@nestjs/schedule';

interface CachedQuote {
  data: DnseQuote;
  updatedAt: number;
}

export class QuoteDnseCacheService {
  // using for debugging
  private readonly logger = new Logger(QuoteDnseCacheService.name);

  // using for caching lastest quotes
  private latestCache = new Map<string, CachedQuote>();

  // life time of cache
  private cacheTTL = 5 * 60 * 1000;

  /**
   * get data from map with symbol
   * get method not check TTL - if expired entry but not cleanupCache() (cron not run yet), get method still return old data
   */
  get(symbol: string): DnseQuote | undefined {
    const cached = this.latestCache.get(symbol);
    if (!cached) return undefined;

    if (Date.now() - cached.updatedAt > this.cacheTTL) {
      this.latestCache.delete(symbol);
      return undefined;
    }

    return cached.data;
  }

  /** save data into caching with updatedAt = new Date() */
  set(symbol: string, data: DnseQuote) {
    this.latestCache.set(symbol, { data, updatedAt: Date.now() });
  }

  /**
   * Cleanup expired cache
   * Loop through the map, delete entry if updatedAt + TTL < now
   * Log number of removed entries
   */
  cleanupCache() {
    const now = Date.now();
    let removed = 0;
    for (const [symbol, { updatedAt }] of this.latestCache.entries()) {
      if (now - updatedAt > this.cacheTTL) {
        this.latestCache.delete(symbol);
        removed++;
      }
    }
    if (removed > 0) {
      this.logger.debug(`Cleaned up ${removed} stale cache entries`);
    }
  }

  /**
   * Assign cron job run each 30 minutes to call cleanupCache()
   * @nestjs/schedule Cron manages this Cron, automatically run following the schedule.
   */
  @Cron(CronExpression.EVERY_30_MINUTES)
  handleCacheCleanup() {
    this.cleanupCache();
  }
}
