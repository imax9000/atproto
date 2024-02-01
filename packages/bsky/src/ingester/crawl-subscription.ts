import { randomIntFromSeed } from '@atproto/crypto'
import { PrimaryDatabase } from '../db'
import { Redis } from '../redis'
import logger from './logger'
import { MINUTE, SECOND, wait } from '@atproto/common'

export class CrawlSubscription {
  ac = new AbortController()
  running: Promise<void> | undefined
  cursor: string | null = null
  maxQueueLen = 100

  constructor(
    public db: PrimaryDatabase,
    public redis: Redis,
    public opts: {
      partitionCount: number
    },
  ) { }

  async start() {
    if (this.running) return
    this.ac = new AbortController()
    this.running = this.run()
      .catch((err) => {
        logger.error({ err }, 'crawl subscription crashed')
      })
      .finally(() => (this.running = undefined))
  }

  private async run() {
    while (!this.ac.signal.aborted) {
      try {
        const ps = await this.check()
        if (ps.length == 0) {
          await wait(5 * SECOND)
          continue
        }
        const need = Object.fromEntries(ps.map((v) => [v.id, this.maxQueueLen - v.len]));
        let total = ps.reduce((sum, v) => sum + this.maxQueueLen - v.len, 0)
        logger.info({need}, 'Need to add %d items to crawl queue', total)
        const stream = this.db.asPrimary().db
          .selectFrom('crawl_state')
          .select('did')
          .where('enqueuedAt', 'is', null)
          .stream()

        let count = 0
        for await (const row of stream) {
          count++
          const part = await randomIntFromSeed(row.did, this.opts.partitionCount)
          if (need[part] > 0) {
            need[part]--
            total--

            logger.trace('scheduling %s to be crawled by partition %d', row.did, part)
            await this.redis.addToStream(crawlPartitionKey(part), '*', [['repo', row.did]])
            await this.db.asPrimary().db
              .updateTable('crawl_state')
              .set({ enqueuedAt: new Date().toISOString() })
              .where('did', '=', row.did)
              .execute()
          }
          if (total <= 0) break
        }
        logger.info('Crawl scheduling run completed, streamed %d rows from DB', count)

      } catch (err) {
        logger.error({err}, 'crawl main loop failed')

        await wait(1000) // wait a second before trying again
      }
    }
  }

  async check() {
    const lens = await this.redis.streamLengths(
      [...Array(this.opts.partitionCount)].map((_, i) => `crawl:${i}`),
    )
    logger.trace({ resp: lens}, 'CrawlSubscription.check')
    for (let i = 0; i < this.opts.partitionCount; i++) {
      if (!lens[i]) lens[i] = 0;
    }
    return lens.map((v, i) => ({ id: i, len: v }))
      .filter((v) => v.len < this.maxQueueLen)
  }

  async destroy() {
    this.ac.abort()
    await this.running
  }

  get destroyed() {
    return this.ac.signal.aborted
  }
}

function crawlPartitionKey(p: number) {
  return `crawl:${p}`
}
