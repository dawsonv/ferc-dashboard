import * as duckdb from '@duckdb/duckdb-wasm';
import duckdb_wasm from '@duckdb/duckdb-wasm/dist/duckdb-mvp.wasm?url';
import mvp_worker from '@duckdb/duckdb-wasm/dist/duckdb-browser-mvp.worker.js?url';
import duckdb_wasm_eh from '@duckdb/duckdb-wasm/dist/duckdb-eh.wasm?url';
import eh_worker from '@duckdb/duckdb-wasm/dist/duckdb-browser-eh.worker.js?url';
import { S3_PATH } from '../data/constants';

const MANUAL_BUNDLES: duckdb.DuckDBBundles = {
  mvp: {
    mainModule: duckdb_wasm,
    mainWorker: mvp_worker,
  },
  eh: {
    mainModule: duckdb_wasm_eh,
    mainWorker: eh_worker,
  },
};

let db: duckdb.AsyncDuckDB | null = null;

export async function getDuckDB() {
  if (db) return db;

  const bundle = await duckdb.selectBundle(MANUAL_BUNDLES);
  const worker = new Worker(bundle.mainWorker!);
  const logger = new duckdb.ConsoleLogger();
  db = new duckdb.AsyncDuckDB(logger, worker);
  await db.instantiate(bundle.mainModule, bundle.pthreadWorker);
  
  // Initialize HTTPFS and S3 settings
  const conn = await db.connect();
  await conn.query(`
    INSTALL httpfs;
    LOAD httpfs;
    SET s3_region='us-west-2';
    SET s3_url_style='path';
    SET threads = 1;
    CREATE VIEW IF NOT EXISTS contracts AS SELECT * FROM read_parquet('${S3_PATH}');
  `);
  await conn.close();
  
  return db;
}
