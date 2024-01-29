import { Kysely } from 'kysely'

export async function up(db: Kysely<unknown>): Promise<void> {
  await db.schema
    .createTable('crawl_state')
    .addColumn('did', 'varchar', (col) => col.primaryKey())
    .addColumn('enqueuedAt', 'varchar')
    .addColumn('completedAt', 'varchar')
    .addColumn('errorMessage', 'text')
    .execute()

  await db.schema
    .createIndex('crawl_state_enqueued_idx')
    .on('crawl_state')
    .column('enqueuedAt')
    .execute()
}

export async function down(db: Kysely<unknown>): Promise<void> {
  await db.schema.dropTable('crawl_state').execute()
}
