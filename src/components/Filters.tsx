import React from 'react';
import { BA_REGIONS, ALL_BAS_FLAT } from '../data/constants';
import type { FilterOptions } from '../lib/queries';
import styles from './Filters.module.css';

interface FiltersProps {
  filters: FilterOptions;
  setFilters: (filters: FilterOptions) => void;
  onClear: () => void;
}

export const Filters: React.FC<FiltersProps> = ({ filters, setFilters, onClear }) => {
  const handleRegionChange = (e: React.ChangeEvent<HTMLSelectElement>) => {
    const region = e.target.value;
    const ba = region === 'All Regions' ? 'All Regions' : `All BAs in ${region}`;
    setFilters({ ...filters, region, ba });
  };

  const baOptions = filters.region === 'All Regions' 
    ? ['All Regions', ...ALL_BAS_FLAT]
    : [`All BAs in ${filters.region}`, ...BA_REGIONS[filters.region]];

  return (
    <div className={styles.filters}>
      <div className={styles.header}>
        <h3>Market Filters</h3>
        <button className={styles.clearBtn} onClick={onClear}>Clear All</button>
      </div>
      
      <div className={styles.field}>
        <label>Product</label>
        <div className={styles.radioGroup}>
          {['ENERGY', 'CAPACITY'].map((p) => (
            <label key={p} className={filters.product === p ? styles.active : ''}>
              <input 
                type="radio" 
                name="product" 
                value={p} 
                checked={filters.product === p} 
                onChange={() => setFilters({ ...filters, product: p as FilterOptions['product'] })}
              />
              {p}
            </label>
          ))}
        </div>
      </div>

      <div className={styles.field}>
        <label>Region</label>
        <select value={filters.region} onChange={handleRegionChange}>
          {Object.keys(BA_REGIONS).map((r) => (
            <option key={r} value={r}>{r}</option>
          ))}
        </select>
      </div>

      <div className={styles.field}>
        <label>Balancing Authority</label>
        <select 
          value={filters.ba} 
          onChange={(e) => setFilters({ ...filters, ba: e.target.value })}
        >
          {baOptions.map((ba) => (
            <option key={ba} value={ba}>{ba}</option>
          ))}
        </select>
      </div>

      <div className={styles.field}>
        <label>Affiliate</label>
        <div className={styles.radioGroup}>
          {['All', 'Affiliate', 'Non-Affiliate'].map((a) => (
            <label key={a} className={filters.affiliate === a ? styles.active : ''}>
              <input 
                type="radio" 
                name="affiliate" 
                value={a} 
                checked={filters.affiliate === a} 
                onChange={() => setFilters({ ...filters, affiliate: a as FilterOptions['affiliate'] })}
              />
              {a}
            </label>
          ))}
        </div>
      </div>

      <div className={styles.field}>
        <label>Service Class</label>
        <div className={styles.radioGroup}>
          {['All', 'Firm', 'Non-Firm'].map((s) => (
            <label key={s} className={filters.serviceType === s ? styles.active : ''}>
              <input 
                type="radio" 
                name="serviceType" 
                value={s} 
                checked={filters.serviceType === s} 
                onChange={() => setFilters({ ...filters, serviceType: s as FilterOptions['serviceType'] })}
              />
              {s}
            </label>
          ))}
        </div>
      </div>

      <div className={styles.field}>
        <label>Rate Basis</label>
        <div className={styles.radioGroup}>
          {['All', 'Market-Based', 'Cost-Based'].map((r) => (
            <label key={r} className={filters.rateType === r ? styles.active : ''}>
              <input 
                type="radio" 
                name="rateType" 
                value={r} 
                checked={filters.rateType === r} 
                onChange={() => setFilters({ ...filters, rateType: r as FilterOptions['rateType'] })}
              />
              {r}
            </label>
          ))}
        </div>
      </div>

      <div className={styles.field}>
        <label>Reporting Years: {filters.yearRange[0]} - {filters.yearRange[1]}</label>
        <input 
          type="range" 
          min="2013" 
          max="2025" 
          value={filters.yearRange[1]}
          onChange={(e) => setFilters({ ...filters, yearRange: [filters.yearRange[0], parseInt(e.target.value)] })}
        />
      </div>
    </div>
  );
};
