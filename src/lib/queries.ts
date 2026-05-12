import { BA_REGIONS } from '../data/constants';

export interface FilterOptions {
  product: 'ENERGY' | 'CAPACITY';
  region: string;
  ba: string;
  affiliate: 'All' | 'Affiliate' | 'Non-Affiliate';
  serviceType: 'All' | 'Firm' | 'Non-Firm';
  rateType: 'All' | 'Market-Based' | 'Cost-Based';
  yearRange: [number, number];
}

export interface TrendData {
  year_quarter: string;
  median_price: number | null;
  p25_price: number | null;
  p75_price: number | null;
  contract_count: number;
}

export interface LeaderboardData {
  entity_name: string;
  total_volume: number;
  contracts_count: number;
}

export function buildWhereClause(filters: FilterOptions) {
  const { product, region, ba, affiliate, serviceType, rateType, yearRange } = filters;
  const unit = product === 'ENERGY' ? '$/MWH' : '$/KW-MO';
  
  const where = [
    `product_name = '${product}'`,
    "term_name = 'LT'",
    `rate_units = '${unit}'`,
    "rate > 0"
  ];
  
  const [startYear, endYear] = yearRange;
  where.push(`CAST(substr(year_quarter, 1, 4) AS INTEGER) BETWEEN ${startYear} AND ${endYear}`);
  
  // Regional Logic
  if (ba !== `All BAs in ${region}` && ba !== "All Regions") {
    where.push(`point_of_delivery_balancing_authority = '${ba}'`);
  } else if (region !== "All Regions") {
    const regionalBas = BA_REGIONS[region];
    const baListStr = regionalBas.join("', '");
    where.push(`point_of_delivery_balancing_authority IN ('${baListStr}')`);
  }
  
  if (affiliate !== "All") {
    const isAffiliate = affiliate === 'Affiliate' ? 'TRUE' : 'FALSE';
    where.push(`contract_affiliate = ${isAffiliate}`);
  }
  
  if (rateType === "Market-Based") {
    where.push("product_type_name = 'MB'");
  } else if (rateType === "Cost-Based") {
    where.push("product_type_name = 'CB'");
  }

  if (serviceType === "Firm") {
    where.push("class_name = 'F'");
  } else if (serviceType === "Non-Firm") {
    where.push("class_name = 'NF'");
  }
    
  return where.join(" AND ");
}

export function getMarketTrendsQuery(filters: FilterOptions) {
  const whereStmt = buildWhereClause(filters);
  return `
    SELECT 
        year_quarter,
        ROUND(approx_quantile(rate, 0.5), 2) as median_price,
        ROUND(approx_quantile(rate, 0.25), 2) as p25_price,
        ROUND(approx_quantile(rate, 0.75), 2) as p75_price,
        CAST(count(*) AS INTEGER) as contract_count
    FROM contracts
    WHERE ${whereStmt}
    GROUP BY 1 ORDER BY 1
  `;
}

export function getLeaderboardQuery(entityType: 'seller' | 'customer', filters: FilterOptions) {
  const whereStmt = buildWhereClause(filters);
  const groupCol = entityType === 'seller' ? 'seller_company_name' : 'customer_company_name';
  
  return `
    SELECT 
        ${groupCol} as entity_name, 
        sum(quantity) as total_volume,
        CAST(count(*) AS INTEGER) as contracts_count
    FROM contracts
    WHERE ${whereStmt}
    GROUP BY 1
    ORDER BY 2 DESC
    LIMIT 10
  `;
}
