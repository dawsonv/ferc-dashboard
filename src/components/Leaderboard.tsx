import React from 'react';
import type { LeaderboardData } from '../lib/queries';
import styles from './Leaderboard.module.css';

interface LeaderboardProps {
  title: string;
  data: LeaderboardData[];
  volumeUnit: string;
}

export const Leaderboard: React.FC<LeaderboardProps> = ({ title, data, volumeUnit }) => {
  return (
    <div className={styles.leaderboard}>
      <h2>{title}</h2>
      <table className={styles.table}>
        <thead>
          <tr>
            <th>Company Name</th>
            <th className={styles.number}>Total Volume ({volumeUnit})</th>
            <th className={styles.number}>Active Contracts</th>
          </tr>
        </thead>
        <tbody>
          {data.length === 0 ? (
            <tr>
              <td colSpan={3} className={styles.empty}>No records found</td>
            </tr>
          ) : (
            data.map((row, i) => (
              <tr key={i}>
                <td title={row.entity_name}>{row.entity_name}</td>
                <td className={styles.number}>{Math.round(row.total_volume).toLocaleString()}</td>
                <td className={styles.number}>{row.contracts_count.toLocaleString()}</td>
              </tr>
            ))
          )}
        </tbody>
      </table>
    </div>
  );
};
