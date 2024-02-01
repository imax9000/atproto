import { PrimaryDatabase } from '../db'
import log from './logger'
import { dbLogger } from '../logger'
import { Redis } from '../redis'
import { IngesterConfig } from './config'
import { IngesterContext } from './context'
import { IngesterSubscription } from './subscription'
import { authWithApiKey, createBsyncClient } from '../bsync'
import { LabelSubscription } from './label-subscription'
import { MuteSubscription } from './mute-subscription'
import { ListReposSubscription } from './list-repos-subscription'
import { CrawlSubscription } from './crawl-subscription'

export { IngesterConfig } from './config'
export type { IngesterConfigValues } from './config'

export class BskyIngester {
  public ctx: IngesterContext
  public sub: IngesterSubscription
  private dbStatsInterval: NodeJS.Timer
  private subStatsInterval: NodeJS.Timer

  constructor(opts: { ctx: IngesterContext; sub: IngesterSubscription }) {
    this.ctx = opts.ctx
    this.sub = opts.sub
  }

  static create(opts: {
    db: PrimaryDatabase
    redis: Redis
    cfg: IngesterConfig
  }): BskyIngester {
    const { db, redis, cfg } = opts
    const bsyncClient = cfg.bsyncUrl
      ? createBsyncClient({
          baseUrl: cfg.bsyncUrl,
          httpVersion: cfg.bsyncHttpVersion ?? '2',
          nodeOptions: { rejectUnauthorized: !cfg.bsyncIgnoreBadTls },
          interceptors: cfg.bsyncApiKey
            ? [authWithApiKey(cfg.bsyncApiKey)]
            : [],
        })
      : undefined
    const labelSubscription = cfg.labelProvider
      ? new LabelSubscription(db, cfg.labelProvider)
      : undefined
    const muteSubscription = bsyncClient
      ? new MuteSubscription(db, redis, bsyncClient)
      : undefined
    const listReposSubscription = new ListReposSubscription(db, cfg.crawlPDSList ?? [])
    const crawlSubscription = new CrawlSubscription(db, redis, {partitionCount: cfg.ingesterPartitionCount})
    const ctx = new IngesterContext({
      db,
      redis,
      cfg,
      labelSubscription,
      muteSubscription,
      listReposSubscription,
      crawlSubscription,
    })
    const sub = new IngesterSubscription(ctx, {
      service: cfg.repoProvider,
      subLockId: cfg.ingesterSubLockId,
      partitionCount: cfg.ingesterPartitionCount,
      maxItems: cfg.ingesterMaxItems,
      checkItemsEveryN: cfg.ingesterCheckItemsEveryN,
      initialCursor: cfg.ingesterInitialCursor,
    })
    return new BskyIngester({ ctx, sub })
  }

  async start() {
    const { db } = this.ctx
    const pool = db.pool
    this.dbStatsInterval = setInterval(() => {
      dbLogger.trace(
        {
          idleCount: pool.idleCount,
          totalCount: pool.totalCount,
          waitingCount: pool.waitingCount,
        },
        'db pool stats',
      )
    }, 10000)
    this.subStatsInterval = setInterval(() => {
      log.trace(
        {
          seq: this.sub.lastSeq,
          streamsLength:
            this.sub.backpressure.lastTotal !== null
              ? this.sub.backpressure.lastTotal
              : undefined,
        },
        'ingester stats',
      )
    }, 500)
    await this.ctx.labelSubscription?.start()
    await this.ctx.muteSubscription?.start()
    await this.ctx.listReposSubscription?.start()
    await this.ctx.crawlSubscription?.start()
    this.sub.run()
    return this
  }

  async destroy(opts?: { skipDb: boolean }): Promise<void> {
    await this.ctx.listReposSubscription?.destroy()
    await this.ctx.crawlSubscription?.destroy()
    await this.ctx.muteSubscription?.destroy()
    await this.ctx.labelSubscription?.destroy()
    await this.sub.destroy()
    clearInterval(this.subStatsInterval)
    await this.ctx.redis.destroy()
    if (!opts?.skipDb) await this.ctx.db.close()
    clearInterval(this.dbStatsInterval)
  }
}

export default BskyIngester
