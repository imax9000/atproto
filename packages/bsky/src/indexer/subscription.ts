import assert from 'node:assert'
import { CID } from 'multiformats/cid'
import { AtUri } from '@atproto/syntax'
import { cborDecode, wait, handleAllSettledErrors, SECOND } from '@atproto/common'
import { DisconnectError } from '@atproto/xrpc-server'
import {
  WriteOpAction,
  readCarWithRoot,
  cborToLexRecord,
  def,
  Commit,
} from '@atproto/repo'
import { ValidationError } from '@atproto/lexicon'
import * as message from '../lexicon/types/com/atproto/sync/subscribeRepos'
import { Leader } from '../db/leader'
import { IndexingService } from '../services/indexing'
import log from './logger'
import {
  ConsecutiveItem,
  ConsecutiveList,
  LatestQueue,
  PartitionedQueue,
  PerfectMap,
  ProcessableMessage,
  jitter,
  loggableMessage,
  strToInt,
} from '../subscription/util'
import IndexerContext from './context'

export const INDEXER_SUB_LOCK_ID = 1200 // need one per partition

export class IndexerSubscription {
  destroyed = false
  leader = new Leader(this.opts.subLockId || INDEXER_SUB_LOCK_ID, this.ctx.db)
  processedCount = 0
  repoQueue = new PartitionedQueue({
    concurrency: this.opts.concurrency ?? Infinity,
  })
  partitions = new PerfectMap<number, Partition>()
  crawlPartitions = new PerfectMap<number, Partition>()
  partitionIds = this.opts.partitionIds
  indexingSvc: IndexingService

  constructor(
    public ctx: IndexerContext,
    public opts: {
      partitionIds: number[]
      subLockId?: number
      concurrency?: number
      partitionBatchSize?: number
    },
  ) {
    this.indexingSvc = ctx.services.indexing(ctx.db)
  }

  async processEvents(opts: { signal: AbortSignal }) {
    const done = () => this.destroyed || opts.signal.aborted
    const batchSize = this.opts.partitionBatchSize ?? 50;
    while (!done()) {
      const results = await this.ctx.redis.readStreams(
        this.partitionIds.map((id) => ({
          key: partitionKey(id),
          cursor: this.partitions.get(id).cursor,
        })),
        {
          blockMs: 1000,
          count: batchSize, // events per stream
        },
      )
      if (done()) break
      for (const { key, messages } of results) {
        const partition = this.partitions.get(partitionId(key))
        for (const msg of messages) {
          const seq = strToInt(msg.cursor)
          const envelope = getEnvelope(msg.contents)
          partition.cursor = seq
          const item = partition.consecutive.push(seq)
          this.repoQueue.add(envelope.repo, async () => {
            await this.handleMessage(partition, item, envelope)
          })
        }
      }

      if (results.length < batchSize * 0.8) {
        const rs = await this.ctx.redis.readStreams(
          this.partitionIds.map((id) => ({
            key: crawlPartitionKey(id),
            cursor: this.crawlPartitions.get(id).cursor,
          })),
          {
            blockMs: 10,
            count: 1,
          },
        )
        log.trace("Got only %d indexing tasks, starting %d crawling tasks", results.length, rs.length)

        for (const { key, messages } of rs) {
          const partition = this.crawlPartitions.get(crawlPartitionId(key))
          for (const msg of messages) {
            const seq = strToInt(msg.cursor)
            assert(msg.contents.repo, 'malformed message content')
            const did = msg.contents.repo.toString()
            partition.cursor = seq
            const item = partition.consecutive.push(seq)

            this.repoQueue.add(did, async () => {
              await this.handleRepoCrawl(partition, item, did)
            })
          }
        }
      }

      await wait(SECOND)
      await this.repoQueue.main.onIdle() // backpressure
    }
  }

  async run() {
    while (!this.destroyed) {
      try {
        const { ran } = await this.leader.run(async ({ signal }) => {
          // initialize cursors to 0 (read from beginning of stream)
          for (const id of this.partitionIds) {
            this.partitions.set(id, new Partition(id, 0))
            this.crawlPartitions.set(id, new Partition(id, 0))
          }
          // process events
          await this.processEvents({ signal })
        })
        if (ran && !this.destroyed) {
          throw new Error('Indexer sub completed, but should be persistent')
        }
      } catch (err) {
        log.error({ err }, 'indexer sub error')
      }
      if (!this.destroyed) {
        await wait(5000 + jitter(1000)) // wait then try to become leader
      }
    }
  }

  async requestReprocess(did: string) {
    await this.repoQueue.add(did, async () => {
      try {
        await this.indexingSvc.indexRepo(did, undefined)
      } catch (err) {
        log.error({ did, err }, 'failed to reprocess repo')
      }
    })
  }

  async destroy() {
    this.destroyed = true
    await this.repoQueue.destroy()
    await Promise.all(
      [...this.partitions.values()].map((p) => p.cursorQueue.destroy()),
    )
    this.leader.destroy(new DisconnectError())
  }

  async resume() {
    this.destroyed = false
    this.partitions = new Map()
    this.repoQueue = new PartitionedQueue({
      concurrency: this.opts.concurrency ?? Infinity,
    })
    await this.run()
  }

  private async handleRepoCrawl(
    partition: Partition,
    item: ConsecutiveItem<number>,
    did: string,
  ) {
    try {
      await this.indexingSvc.indexRepo(did, undefined)
      await this.ctx.db.asPrimary().db.updateTable('crawl_state')
        .set({ completedAt: new Date().toISOString() })
        .where('did', '=', did)
        .execute()
    } catch (err) {
      log.error({ did, err }, 'failed to reprocess repo')
      await this.ctx.db.asPrimary().db.updateTable('crawl_state')
        .set({ completedAt: new Date().toISOString(), errorMessage: (err as Error).toString() })
        .where('did', '=', did)
        .execute()
    } finally {
      const latest = item.complete().at(-1)
      if (latest !== undefined) {
        partition.cursorQueue
          .add(async () => {
            await this.ctx.redis.trimStream(crawlPartitionKey(partition.id), latest + 1)
          })
          .catch((err) => {
            log.error({ err }, 'indexer cursor error')
          })
      }
    }
  }

  private async handleMessage(
    partition: Partition,
    item: ConsecutiveItem<number>,
    envelope: Envelope,
  ) {
    const msg = envelope.event
    try {
      if (message.isCommit(msg)) {
        await this.handleCommit(msg)
      } else if (message.isHandle(msg)) {
        await this.handleUpdateHandle(msg)
      } else if (message.isTombstone(msg)) {
        await this.handleTombstone(msg)
      } else if (message.isMigrate(msg)) {
        // Ignore migrations
      } else {
        const exhaustiveCheck: never = msg
        throw new Error(`Unhandled message type: ${exhaustiveCheck['$type']}`)
      }
    } catch (err) {
      // We log messages we can't process and move on:
      // otherwise the cursor would get stuck on a poison message.
      log.error(
        { err, message: loggableMessage(msg) },
        'indexer message processing error',
      )
    } finally {
      this.processedCount++
      const latest = item.complete().at(-1)
      if (latest !== undefined) {
        partition.cursorQueue
          .add(async () => {
            await this.ctx.redis.trimStream(partition.key, latest + 1)
          })
          .catch((err) => {
            log.error({ err }, 'indexer cursor error')
          })
      }
    }
  }

  private async handleCommit(msg: message.Commit) {
    const indexRecords = async () => {
      const { root, rootCid, ops } = await getOps(msg)
      if (msg.tooBig) {
        await this.indexingSvc.indexRepo(msg.repo, rootCid.toString())
        await this.indexingSvc.setCommitLastSeen(root, msg)
        return
      }
      if (msg.rebase) {
        const needsReindex = await this.indexingSvc.checkCommitNeedsIndexing(
          root,
        )
        if (needsReindex) {
          await this.indexingSvc.indexRepo(msg.repo, rootCid.toString())
        }
        await this.indexingSvc.setCommitLastSeen(root, msg)
        return
      }
      for (const op of ops) {
        if (op.action === WriteOpAction.Delete) {
          await this.indexingSvc.deleteRecord(op.uri)
        } else {
          try {
            await this.indexingSvc.indexRecord(
              op.uri,
              op.cid,
              op.record,
              op.action, // create or update
              msg.time,
            )
          } catch (err) {
            if (err instanceof ValidationError) {
              log.warn(
                {
                  did: msg.repo,
                  commit: msg.commit.toString(),
                  uri: op.uri.toString(),
                  cid: op.cid.toString(),
                },
                'skipping indexing of invalid record',
              )
            } else {
              log.error(
                {
                  err,
                  did: msg.repo,
                  commit: msg.commit.toString(),
                  uri: op.uri.toString(),
                  cid: op.cid.toString(),
                },
                'skipping indexing due to error processing record',
              )
            }
          }
        }
      }
      await this.indexingSvc.setCommitLastSeen(root, msg)
    }
    const results = await Promise.allSettled([
      indexRecords(),
      this.indexingSvc.indexHandle(msg.repo, msg.time),
    ])
    handleAllSettledErrors(results)
  }

  private async handleUpdateHandle(msg: message.Handle) {
    await this.indexingSvc.indexHandle(msg.did, msg.time, true)
  }

  private async handleTombstone(msg: message.Tombstone) {
    await this.indexingSvc.tombstoneActor(msg.did)
  }
}

async function getOps(
  msg: message.Commit,
): Promise<{ root: Commit; rootCid: CID; ops: PreparedWrite[] }> {
  const car = await readCarWithRoot(msg.blocks as Uint8Array)
  const rootBytes = car.blocks.get(car.root)
  assert(rootBytes, 'Missing commit block in car slice')

  const root = def.commit.schema.parse(cborDecode(rootBytes))
  const ops: PreparedWrite[] = msg.ops.map((op) => {
    const [collection, rkey] = op.path.split('/')
    assert(collection && rkey)
    if (
      op.action === WriteOpAction.Create ||
      op.action === WriteOpAction.Update
    ) {
      assert(op.cid)
      const record = car.blocks.get(op.cid)
      assert(record)
      return {
        action:
          op.action === WriteOpAction.Create
            ? WriteOpAction.Create
            : WriteOpAction.Update,
        cid: op.cid,
        record: cborToLexRecord(record),
        blobs: [],
        uri: AtUri.make(msg.repo, collection, rkey),
      }
    } else if (op.action === WriteOpAction.Delete) {
      return {
        action: WriteOpAction.Delete,
        uri: AtUri.make(msg.repo, collection, rkey),
      }
    } else {
      throw new Error(`Unknown repo op action: ${op.action}`)
    }
  })

  return { root, rootCid: car.root, ops }
}

function getEnvelope(val: Record<string, Buffer | undefined>): Envelope {
  assert(val.repo && val.event, 'malformed message contents')
  return {
    repo: val.repo.toString(),
    event: cborDecode(val.event) as ProcessableMessage,
  }
}

type Envelope = {
  repo: string
  event: ProcessableMessage
}

class Partition {
  consecutive = new ConsecutiveList<number>()
  cursorQueue = new LatestQueue()
  constructor(public id: number, public cursor: number) {}
  get key() {
    return partitionKey(this.id)
  }
}

function partitionId(key: string) {
  assert(key.startsWith('repo:'))
  return strToInt(key.replace('repo:', ''))
}

function crawlPartitionId(key: string) {
  assert(key.startsWith('crawl:'))
  return strToInt(key.replace('crawl:', ''))
}

function partitionKey(p: number) {
  return `repo:${p}`
}

function crawlPartitionKey(p: number) {
  return `crawl:${p}`
}

type PreparedCreate = {
  action: WriteOpAction.Create
  uri: AtUri
  cid: CID
  record: Record<string, unknown>
  blobs: CID[] // differs from similar type in pds
}

type PreparedUpdate = {
  action: WriteOpAction.Update
  uri: AtUri
  cid: CID
  record: Record<string, unknown>
  blobs: CID[] // differs from similar type in pds
}

type PreparedDelete = {
  action: WriteOpAction.Delete
  uri: AtUri
}

type PreparedWrite = PreparedCreate | PreparedUpdate | PreparedDelete
