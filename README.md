# FERC EQR Explorer

A modern, professional-quality dashboard for analyzing long-term Power Purchase Agreements (PPAs) and capacity contracts using data from FERC Electric Quarterly Reports (EQRs).

## Tech Stack
- **Frontend:** React (TypeScript) via Vite
- **Data Engine:** DuckDB-WASM (Client-side SQL over S3 Parquet)
- **Visualizations:** Recharts (D3-based)
- **Styling:** Vanilla CSS Modules
- **Hosting:** Optimized for Cloudflare Pages (100% Static)

## Key Features
- **Zero-Backend Architecture:** Queries the [PUDL](https://github.com/catalyst-cooperative/pudl) S3 data lake directly from your browser.
- **High Performance:** Leverages DuckDB-WASM for blazing-fast analytical queries on large datasets.
- **Modern UI:** Clean, responsive design with interactive charts and market leaderboards.

## Getting Started

### Local Development
1. Install dependencies:
   ```bash
   npm install
   ```
2. Start the dev server:
   ```bash
   npm run dev
   ```

### Deployment to Cloudflare Pages
This app is designed to be hosted as a 100% static site on Cloudflare Pages.

#### 1. Via Wrangler CLI (Recommended)
The fastest way to deploy is using the Wrangler CLI:

1. Build the production assets:
   ```bash
   npm run build
   ```
2. Deploy the `dist` folder:
   ```bash
   npx wrangler pages deploy dist
   ```
   *Follow the terminal prompts to log in and select/create your project.*

#### 2. Via Cloudflare Dashboard
Alternatively, you can connect your GitHub repository directly:
1. Go to **Workers & Pages** > **Pages** > **Connect to Git**.
2. Select this repository.
3. Set **Build command** to `npm run build`.
4. Set **Build output directory** to `dist`.
5. Click **Save and Deploy**.

---
Data provided by [PUDL](https://github.com/catalyst-cooperative/pudl). Created by Dawson Verley.
