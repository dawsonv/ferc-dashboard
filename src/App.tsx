import React, { useState, useEffect, useCallback } from 'react';
import { Filters } from './components/Filters';
import { TrendsChart } from './components/TrendsChart';
import { Leaderboard } from './components/Leaderboard';
import { useDashboardData } from './hooks/useDashboardData';
import type { FilterOptions } from './lib/queries';
import styles from './App.module.css';
import './styles/globals.css';

import { FileDown } from 'lucide-react';

const DEFAULT_FILTERS: FilterOptions = {
  product: 'ENERGY',
  region: 'All Regions',
  ba: 'All Regions',
  affiliate: 'All',
  serviceType: 'All',
  rateType: 'All',
  yearRange: [2013, 2025],
};

const App: React.FC = () => {
  // Initialize state from URL if present
  const [filters, setFilters] = useState<FilterOptions>(() => {
    const params = new URLSearchParams(window.location.search);
    if (params.toString() === '') return DEFAULT_FILTERS;

    return {
      product: (params.get('product') as FilterOptions['product']) || DEFAULT_FILTERS.product,
      region: params.get('region') || DEFAULT_FILTERS.region,
      ba: params.get('ba') || DEFAULT_FILTERS.ba,
      affiliate: (params.get('affiliate') as FilterOptions['affiliate']) || DEFAULT_FILTERS.affiliate,
      serviceType: (params.get('service') as FilterOptions['serviceType']) || DEFAULT_FILTERS.serviceType,
      rateType: (params.get('rate') as FilterOptions['rateType']) || DEFAULT_FILTERS.rateType,
      yearRange: [
        parseInt(params.get('start') || '2013'),
        parseInt(params.get('end') || '2025')
      ],
    };
  });

  // Sync state to URL
  useEffect(() => {
    const params = new URLSearchParams();
    params.set('product', filters.product);
    params.set('region', filters.region);
    params.set('ba', filters.ba);
    params.set('affiliate', filters.affiliate);
    params.set('service', filters.serviceType);
    params.set('rate', filters.rateType);
    params.set('start', filters.yearRange[0].toString());
    params.set('end', filters.yearRange[1].toString());

    const newUrl = `${window.location.pathname}?${params.toString()}`;
    window.history.replaceState({}, '', newUrl);
  }, [filters]);

  const clearFilters = useCallback(() => {
    setFilters(DEFAULT_FILTERS);
  }, []);

  const { trends, topSellers, topBuyers, loading, error } = useDashboardData(filters);

  const handleExport = () => {
    window.print();
  };

  const unitLabel = filters.product === 'ENERGY' ? '$/MWh' : '$/kW-mo';
  const volUnit = filters.product === 'ENERGY' ? 'MWh' : 'MW-Mo';

  return (
    <div className={styles.container}>
      <aside className={styles.sidebar}>
        <div className={styles.brand}>powerpurchasing.work</div>
        <Filters filters={filters} setFilters={setFilters} onClear={clearFilters} />
      </aside>

      <main className={styles.main}>
        {loading && (
          <div className={styles.loadingOverlay}>
            Querying PUDL S3 Lake...
          </div>
        )}

        <header className={styles.header}>
          <div className={styles.headerMain}>
            <h1>PPA Dashboard</h1>
            <p>
              Analyze long-term Power Purchase Agreement (PPA) price trends using data from FERC Electric Quarterly Reports.
              <br></br>
              Dashboard by Dawson Verley (<a href="https://dawsonv.github.io">dawsonv.github.io</a>)
            </p>
          </div>
          <button className={styles.exportBtn} onClick={handleExport} title="Export to PDF">
            <FileDown size={18} />
            <span>Export PDF</span>
          </button>
        </header>

        {error && (
          <div className={styles.card} style={{ borderColor: '#ef4444', color: '#ef4444' }}>
            <h2>Error Loading Data</h2>
            <p>{error}</p>
          </div>
        )}

        <div className={styles.grid}>
          <section className={styles.card}>
            <h2>{filters.ba === 'All Regions' ? 'Regional Price Index' : `${filters.ba} Price Trends`}</h2>
            <TrendsChart data={trends} unit={unitLabel} />
          </section>

          <div className={styles.leaderboardGrid}>
            <section className={styles.card}>
              <Leaderboard 
                title="Top Sellers" 
                data={topSellers} 
                volumeUnit={volUnit} 
              />
            </section>
            <section className={styles.card}>
              <Leaderboard 
                title="Top Buyers" 
                data={topBuyers} 
                volumeUnit={volUnit} 
              />
            </section>
          </div>
        </div>
      </main>
    </div>
  );
};

export default App;
