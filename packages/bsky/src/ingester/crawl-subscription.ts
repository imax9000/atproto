import { randomIntFromSeed } from '@atproto/crypto'
import { PrimaryDatabase } from '../db'
import { Redis } from '../redis'
import logger from './logger'
import { MINUTE, wait } from '@atproto/common'

export class CrawlSubscription {
  ac = new AbortController()
  running: Promise<void> | undefined
  cursor: string | null = null
  maxQueueLen = 5

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
          await wait(5 * MINUTE)
          continue
        }
        const need = Object.fromEntries(ps.map((v) => [v.id, this.maxQueueLen - v.len]));
        let total = ps.reduce((sum, v) => sum + v.len, 0)
        const stream = this.db.asPrimary().db
          .selectFrom('crawl_state')
          .select('did')
          .where('enqueuedAt', 'is', 'null')
          .stream()

        for await (const row of stream) {
          const part = await randomIntFromSeed(row.did, this.opts.partitionCount)
          if (need[part] > 0) {
            need[part]--
            total--

            await this.redis.addToStream(crawlPartitionKey(part), row.did, [['repo', row.did]])
            await this.db.asPrimary().db
              .updateTable('crawl_state')
              .set({ enqueuedAt: new Date().toISOString() })
              .where('did', '=', row.did)
          }
          if (total <= 0) break
        }

      } catch (err) {

        await wait(1000) // wait a second before trying again
      }
    }
  }

  async check() {
    const lens = await this.redis.streamLengths(
      [...Array(this.opts.partitionCount)].map((_, i) => `crawl:${i}`),
    )
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
