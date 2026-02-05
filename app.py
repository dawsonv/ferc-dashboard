import streamlit as st
import duckdb
import pandas as pd
from plotly.subplots import make_subplots
import plotly.graph_objects as go

# ==========================================
# 1. CONFIGURATION & SETUP
# ==========================================
st.set_page_config(
    page_title="FERC EQR Explorer", 
    layout="wide",
    initial_sidebar_state="collapsed"
)

st.title("FERC EQR Explorer")
st.markdown("""
    **PPA Market Intelligence:** Analyze trends in long-term Power Purchase Agreements (PPAs) 
    and capacity contracts using data from FERC Electric Quarterly Reports (EQRs). 
    Made possible by open data from [PUDL](https://github.com/catalyst-cooperative/pudl). 
    Dashboard created by [Dawson Verley](https://dawsonv.github.io).
    **Note:** The FERC data is preliminary and may contain errors or omissions.
""")

# ==========================================
# 2. REGIONAL MAPPING
# ==========================================
BA_REGIONS = {
    "All Regions": ["All Regions"],
    "California (CISO)": ["CISO", "BANC", "IID", "LDWP", "TIDC", "WALC"],
    "Northwest (NW)": ["BPAT", "AVA", "CHPD", "DOPD", "GCPD", "GRID", "IPCO", "NWMT", "PACE", "PACW", "PGE", "PSEI", "SCL", "TPWR", "WAUW"],
    "Southwest (SW)": ["AZPS", "EPE", "NEVP", "PNM", "SRP", "TEPC", "WALC", "WACM"],
    # Removed ERCOT (Texas) as it is non-jurisdictional
    "Midwest (MISO)": ["MISO", "ALTE", "ALTW", "AMIL", "AMMO", "AMRN", "BREC", "CIN", "CONS", "DECO", "DPC", "GRE", "HE", "IPL", "MEC", "MECS", "MP", "NSP", "OTP", "SIGE", "SMP", "UPPC", "WEC", "WPS"],
    "Central (SPP)": ["SPP", "AECI", "EDE", "GRDA", "INDN", "KCPL", "LES", "NPPD", "OKGE", "OPPD", "SPA", "SPS", "SWPP", "WAUE", "WFEC", "WR"],
    "Southeast (SE)": ["SOCO", "AEC", "CPLE", "CPLW", "DUK", "FPC", "FPL", "GVL", "HST", "JEA", "LGEE", "SC", "SCEG", "TAL", "TEC", "TVA", "YAD"],
    "Mid-Atlantic (PJM)": ["PJM", "AEBN", "AEP", "AP", "BC", "CE", "DAY", "DE", "DL", "DOM", "DPL", "DUQ", "EKPC", "FE", "JC", "ME", "PE", "PEP", "PL", "PN", "PS", "RE"],
    "New York (NYISO)": ["NYIS"],
    "New England (ISONE)": ["ISNE"],
    "Canada / Other": ["AESO", "BCHA", "CFE", "HQT", "IESO", "MHEB", "NBSO", "SPC"]
}

# Helper to flatten lists for "All Regions" queries
ALL_BAS_FLAT = sorted({ba for bas in BA_REGIONS.values() for ba in bas if ba != "All Regions"})

# ==========================================
# 3. OPTIMIZED DATABASE CONNECTION
# ==========================================
@st.cache_resource
def get_con():
    con = duckdb.connect()
    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute("SET s3_region='us-west-2';")
    con.execute("SET threads = 8;") 
    con.execute("SET s3_url_style='path';") 
    return con

# ==========================================
# 4. QUERY LOGIC
# ==========================================
def build_where_clause(product, region, ba_selection, affiliate, rate_type, service_type, year_range):
    unit = '$/MWH' if product == 'ENERGY' else '$/KW-MO'
    
    where = [
        f"product_name = '{product}'",
        "term_name = 'LT'",
        f"rate_units = '{unit}'",
        "rate > 0"
    ]
    
    start_year, end_year = year_range
    where.append(f"CAST(substr(year_quarter, 1, 4) AS INTEGER) BETWEEN {start_year} AND {end_year}")
    
    # Regional Logic
    if ba_selection != f"All BAs in {region}" and ba_selection != "All Regions":
        where.append(f"point_of_delivery_balancing_authority = '{ba_selection}'")
    elif region != "All Regions":
        regional_bas = BA_REGIONS[region]
        ba_list_str = "', '".join(regional_bas)
        where.append(f"point_of_delivery_balancing_authority IN ('{ba_list_str}')")
        
    if affiliate != "All":
        is_affiliate = 'TRUE' if affiliate == 'Affiliate' else 'FALSE'
        where.append(f"contract_affiliate = {is_affiliate}")
        
    # Rate Type Logic
    if rate_type == "Market-Based":
        where.append("product_type_name = 'MB'")
    elif rate_type == "Cost-Based":
        where.append("product_type_name = 'CB'")

    # Service Class Logic (Hard vs. Soft)
    if service_type == "Firm":
        where.append("class_name = 'F'")
    elif service_type == "Non-Firm":
        where.append("class_name = 'NF'")
    # Note: 'All' includes F, NF, UP (Unit Power), etc.
        
    return " AND ".join(where)

def fetch_market_trends(product, region, ba, affiliate, rate_type, service_type, year_range):
    con = get_con()
    s3_path = "s3://pudl.catalyst.coop/ferceqr/core_ferceqr__contracts/*.parquet"
    where_stmt = build_where_clause(product, region, ba, affiliate, rate_type, service_type, year_range)

    query = f"""
    SELECT 
        year_quarter,
        approx_quantile(rate, 0.5) as median_price,
        approx_quantile(rate, 0.25) as p25_price,
        approx_quantile(rate, 0.75) as p75_price,
        count(*) as contract_count
    FROM read_parquet('{s3_path}')
    WHERE {where_stmt}
    GROUP BY 1 ORDER BY 1
    """
    return con.execute(query).df(), query

def fetch_leaderboard(entity_type, product, region, ba, affiliate, rate_type, service_type, year_range):
    con = get_con()
    s3_path = "s3://pudl.catalyst.coop/ferceqr/core_ferceqr__contracts/*.parquet"
    where_stmt = build_where_clause(product, region, ba, affiliate, rate_type, service_type, year_range)
    
    group_col = "seller_company_name" if entity_type == "seller" else "customer_company_name"

    query = f"""
    SELECT 
        {group_col} as entity_name, 
        sum(quantity) as total_volume,
        count(*) as contracts_count
    FROM read_parquet('{s3_path}')
    WHERE {where_stmt}
    GROUP BY 1
    ORDER BY 2 DESC
    LIMIT 10
    """
    return con.execute(query).df()

# ==========================================
# 5. DASHBOARD UI LAYOUT
# ==========================================

st.write("### 1. Market Filters")
# Added extra column for Service Class
col_filters = st.columns([0.8, 1, 1.2, 1, 1, 1, 1.5]) 

with col_filters[0]:
    product_choice = st.radio("Product", ["ENERGY", "CAPACITY"], horizontal=True)

with col_filters[1]:
    region_choice = st.selectbox("Region", list(BA_REGIONS.keys()))

with col_filters[2]:
    if region_choice == "All Regions":
        ba_options = ["All Regions"] + ALL_BAS_FLAT
    else:
        ba_options = [f"All BAs in {region_choice}"] + sorted(BA_REGIONS[region_choice])
    ba_choice = st.selectbox("Balancing Authority", ba_options)

with col_filters[3]:
    affiliate_choice = st.radio("Affiliate", ["All", "Affiliate", "Non-Affiliate"], horizontal=True)

with col_filters[4]:
    # Hard (Firm) vs Soft (Non-Firm)
    service_choice = st.radio("Service Class", ["All", "Firm", "Non-Firm"], horizontal=True)

with col_filters[5]:
    rate_choice = st.radio("Rate Basis", ["All", "Market-Based", "Cost-Based"], horizontal=True)

with col_filters[6]:
    year_range = st.slider("Reporting Years", 2013, 2025, (2013, 2025))

# --- EXECUTION ---
with st.spinner(f"Querying PUDL S3 Lake ({year_range[0]}-{year_range[1]})..."):
    df_trends, debug_query = fetch_market_trends(product_choice, region_choice, ba_choice, affiliate_choice, rate_choice, service_choice, year_range)

# --- VISUALIZATION ---
if not df_trends.empty:
    unit_label = "$/MWh" if product_choice == 'ENERGY' else "$/kW-mo"
    vol_unit = "MWh" if product_choice == 'ENERGY' else "MW-Mo"
    
    chart_title = f"{ba_choice} Price Trends" if "All BAs" not in ba_choice else f"{region_choice} Regional Price Index"

    fig = make_subplots(
        rows=2, cols=1, 
        shared_xaxes=True, 
        vertical_spacing=0.08, 
        row_heights=[0.7, 0.3],
        subplot_titles=(f"Median Price ({unit_label})", "Contract Volume")
    )

    # 1. Price Traces
    fig.add_trace(go.Scatter(
        x=df_trends['year_quarter'], y=df_trends['p75_price'],
        mode='lines', line=dict(width=0), showlegend=False, hoverinfo='skip'
    ), row=1, col=1)
    
    fig.add_trace(go.Scatter(
        x=df_trends['year_quarter'], y=df_trends['p25_price'],
        mode='lines', line=dict(width=0),
        fill='tonexty', fillcolor='rgba(0, 176, 246, 0.2)',
        name='IQR (P25-P75)'
    ), row=1, col=1)
    
    fig.add_trace(go.Scatter(
        x=df_trends['year_quarter'], y=df_trends['median_price'],
        mode='lines+markers', line=dict(color='rgb(0, 176, 246)', width=3),
        name='Median Price'
    ), row=1, col=1)

    # 2. Volume Traces
    fig.add_trace(go.Bar(
        x=df_trends['year_quarter'], y=df_trends['contract_count'],
        marker_color='rgba(160, 160, 160, 0.6)',
        name='Count'
    ), row=2, col=1)

    # Layout: Diagonal labels & Compact margins
    fig.update_layout(
        title=chart_title,
        height=600, 
        template='plotly_white', 
        hovermode='x unified',
        xaxis_tickangle=-45,
        legend=dict(
            yanchor="top", y=0.99,
            xanchor="left", x=0.01,
            bgcolor="rgba(255, 255, 255, 0.8)",
            bordercolor="rgba(0,0,0,0.1)",
            borderwidth=1
        ),
        margin=dict(l=10, r=10, t=40, b=10)
    )
    st.plotly_chart(fig, width='stretch')

    # --- LEADERBOARDS ---
    st.write(f"### Market Leaders: {ba_choice}")
    
    cols_display = {
        "entity_name": "Company Name",
        "total_volume": f"Total Volume ({vol_unit})",
        "contracts_count": "Active Contracts"
    }

    col_lead_1, col_lead_2 = st.columns(2)

    with col_lead_1:
        st.subheader("Top Sellers")
        df_s = fetch_leaderboard("seller", product_choice, region_choice, ba_choice, affiliate_choice, rate_choice, service_choice, year_range)
        df_s = df_s.rename(columns=cols_display)
        st.dataframe(
            df_s.style.format({
                f"Total Volume ({vol_unit})": "{:,.0f}", 
                "Active Contracts": "{:,.0f}"
            }), 
            use_container_width=True, 
            hide_index=True
        )

    with col_lead_2:
        st.subheader("Top Buyers")
        df_b = fetch_leaderboard("buyer", product_choice, region_choice, ba_choice, affiliate_choice, rate_choice, service_choice, year_range)
        df_b = df_b.rename(columns=cols_display)
        st.dataframe(
            df_b.style.format({
                f"Total Volume ({vol_unit})": "{:,.0f}", 
                "Active Contracts": "{:,.0f}"
            }), 
            use_container_width=True, 
            hide_index=True
        )

else:
    st.warning(f"⚠️ No data found for {ba_choice}. Try selecting a different BA or broadening the filters.")

with st.expander("🛠️ View Generated SQL (Debug)"):
    st.code(debug_query, language="sql")