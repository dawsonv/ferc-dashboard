import { useState, useEffect, useCallback } from 'react';
import { useDuckDB } from './useDuckDB';
import { getMarketTrendsQuery, getLeaderboardQuery } from '../lib/queries';
import type { FilterOptions, TrendData, LeaderboardData } from '../lib/queries';

export function useDashboardData(filters: FilterOptions) {
  const { db, loading: dbLoading, error: dbError } = useDuckDB();
  const [data, setData] = useState<{
    trends: TrendData[];
    topSellers: LeaderboardData[];
    topBuyers: LeaderboardData[];
  }>({ trends: [], topSellers: [], topBuyers: [] });
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const fetchData = useCallback(async () => {
    if (!db) return;
    setLoading(true);
    setError(null);

    try {
      const conn = await db.connect();

      // DuckDB-WASM often needs these explicitly set in the session to work with S3 globs
      await conn.query(`
        SET s3_region='us-west-2';
        SET s3_url_style='path';
        SET threads = 1;
      `);

      const [trendsResult, sellersResult, buyersResult] = await Promise.all([
        conn.query(getMarketTrendsQuery(filters)),
        conn.query(getLeaderboardQuery('seller', filters)),
        conn.query(getLeaderboardQuery('customer', filters))
      ]);

      setData({
        trends: trendsResult.toArray().map(row => row.toJSON() as TrendData),
        topSellers: sellersResult.toArray().map(row => row.toJSON() as LeaderboardData),
        topBuyers: buyersResult.toArray().map(row => row.toJSON() as LeaderboardData),
      });

      await conn.close();
    } catch (err) {
      console.error('Query error:', err);
      setError(err instanceof Error ? err.message : 'Query failed');
    } finally {
      setLoading(false);
    }
  }, [db, filters]);

  useEffect(() => {
    if (!db) return;

    const debounceTimeout = setTimeout(() => {
      fetchData();
    }, 300);

    return () => clearTimeout(debounceTimeout);
  }, [fetchData, db]);

  return {
    ...data,
    loading: dbLoading || loading,
    error: (dbError instanceof Error ? dbError.message : dbError) || error,
  };
}
