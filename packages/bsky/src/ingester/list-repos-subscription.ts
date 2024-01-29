import { PrimaryDatabase } from '../db'
import logger from './logger'
import { HOUR } from '@atproto/common'
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
      return new AtpAgent({service: u})
    })
  }

  async start() {
    this.poll()
  }

  poll() {
    if (this.destroyed) return
    this.promise = this.listRepos()
      .catch((err) =>
        logger.error({ err }, 'failed to list repos'),
      )
      .finally(() => {
        this.timer = setTimeout(() => this.poll(), 6 * HOUR)
      })
  }

  async listRepos() {
    for (const agent of this.pdsAgents) {
      const res = await agent.com.atproto.sync.listRepos()
      const vals = res.data.repos.map((r) => ({did: r.did}))
      await this.db.asPrimary().db
        .insertInto('crawl_state')
        .values(vals)
        .onConflict((oc) => oc.doNothing())
        .execute()
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
