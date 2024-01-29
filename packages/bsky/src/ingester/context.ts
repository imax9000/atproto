import { PrimaryDatabase } from '../db'
import { Redis } from '../redis'
import { IngesterConfig } from './config'
import { CrawlSubscription } from './crawl-subscription'
import { LabelSubscription } from './label-subscription'
import { ListReposSubscription } from './list-repos-subscription'
import { MuteSubscription } from './mute-subscription'

export class IngesterContext {
  constructor(
    private opts: {
      db: PrimaryDatabase
      redis: Redis
      cfg: IngesterConfig
      labelSubscription?: LabelSubscription
      muteSubscription?: MuteSubscription
      listReposSubscription?: ListReposSubscription
      crawlSubscription?: CrawlSubscription
    },
  ) {}

  get db(): PrimaryDatabase {
    return this.opts.db
  }

  get redis(): Redis {
    return this.opts.redis
  }

  get cfg(): IngesterConfig {
    return this.opts.cfg
  }

  get labelSubscription(): LabelSubscription | undefined {
    return this.opts.labelSubscription
  }

  get muteSubscription(): MuteSubscription | undefined {
    return this.opts.muteSubscription
  }

  get listReposSubscription() {
    return this.opts.listReposSubscription
  }

  get crawlSubscription() {
    return this.opts.crawlSubscription
  }
}

export default IngesterContext
