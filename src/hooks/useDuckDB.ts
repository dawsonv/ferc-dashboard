import { useState, useEffect } from 'react';
import { getDuckDB } from '../lib/duckdb';
import { AsyncDuckDB } from '@duckdb/duckdb-wasm';

export function useDuckDB() {
  const [db, setDb] = useState<AsyncDuckDB | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<Error | null>(null);

  useEffect(() => {
    getDuckDB()
      .then(setDb)
      .catch(setError)
      .finally(() => setLoading(false));
  }, []);

  return { db, loading, error };
}
