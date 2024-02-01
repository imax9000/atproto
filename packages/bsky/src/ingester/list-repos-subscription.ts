import { PrimaryDatabase } from '../db'
import logger from './logger'
import { HOUR, MINUTE } from '@atproto/common'
import AtpAgent from '@atproto/api'

export class ListReposSubscription {
  destroyed = false
  timer: NodeJS.Timer | undefined
  promise: Promise<void> = Promise.resolve()
  pdsAgents: AtpAgent[]

  constructor(
    public db: PrimaryDatabase,
    public pds: string[],
  ) {
    this.pdsAgents = pds.map((u) => {
      return new AtpAgent({ service: u })
    })
  }

  async start() {
    this.poll()
  }

  poll() {
    logger.info('ListReposSubscription.poll start')
    if (this.destroyed) {
      logger.info('ListReposSubscription destroyed')
      return
    }
    this.promise = this.listRepos()
      .catch((err) =>
        logger.error({ err }, 'failed to list repos'),
      )
      .finally(() => {
        this.timer = setTimeout(() => this.poll(), 3 * HOUR)
      })
  }

  async listRepos() {
    for (const agent of this.pdsAgents) {
      let cursor: string | undefined = ''
      let listed = 0, added = 0
      do {
        const res = await agent.com.atproto.sync.listRepos({ cursor })
        const vals = res.data.repos.map((r) => ({ did: r.did }))
        listed += vals.length
        if (vals.length > 0) {
          const res = await this.db.asPrimary().db
            .insertInto('crawl_state')
            .values(vals)
            .onConflict((oc) => oc.doNothing())
            .execute()
          added += res.length
        }
        cursor = res.data.cursor
      } while (cursor)
      logger.info('Completed listing repos from %s: %d listed, %d added', agent.service, listed, added)
    }
  }

  async destroy() {
    this.destroyed = true
    if (this.timer) {
      clearTimeout(this.timer)
    }
    await this.promise
  }
}
