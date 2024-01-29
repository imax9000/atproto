export interface CrawlState {
  did: string
  enqueuedAt: string | null
  completedAt: string | null
  errorMessage: string | null
}

export const tableName = 'crawl_state'

export type PartialDB = { [tableName]: CrawlState }
