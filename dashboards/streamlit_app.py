"""Binance Market Analytics dashboard — reads dbt model fact_daily_metrics from BigQuery.

Run:  streamlit run streamlit_app.py
Auth: set GOOGLE_APPLICATION_CREDENTIALS env var, or paste a service-account
      JSON into .streamlit/secrets.toml (see secrets.toml.example).
"""
import os
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from google.cloud import bigquery

st.set_page_config(page_title="Binance Market Analytics", page_icon="📈", layout="wide")

PLOTLY_LAYOUT = dict(
    template="plotly_dark",
    paper_bgcolor="rgba(0,0,0,0)",
    plot_bgcolor="rgba(0,0,0,0)",
    font=dict(color="#E6E6E6"),
    margin=dict(l=10, r=10, t=40, b=10),
    hovermode="x unified",
)
GOLD, RED, GREEN = "#F0B90B", "#F6465D", "#0ECB81"   # Binance palette

METRICS = {
    "Closing price": "closing_price",
    "Daily return %": "daily_return_pct",
    "Volatility (7d)": "volatility_7d",
    "Quote volume": "quote_volume",
}

# ------------------------------------------------------------------ data layer
@st.cache_resource
def bq_client():
    try:
        if "gcp_service_account" in st.secrets:
            from google.oauth2 import service_account
            creds = service_account.Credentials.from_service_account_info(
                dict(st.secrets["gcp_service_account"]))
            return bigquery.Client(project=st.secrets.get("project"), credentials=creds)
    except Exception:
        pass
    return bigquery.Client()  # falls back to GOOGLE_APPLICATION_CREDENTIALS / ADC


@st.cache_data(ttl=3600, show_spinner="Loading symbols…")
def load_symbols(_client, dataset):
    sql = f"SELECT DISTINCT symbol FROM `{dataset}.fact_daily_metrics` ORDER BY symbol"
    return [r.symbol for r in _client.query(sql).result()]


@st.cache_data(ttl=3600, show_spinner="Querying BigQuery…")
def load_data(_client, dataset, symbols, start, end):
    job = _client.query(
        f"""SELECT symbol, open_date, closing_price, volume, quote_volume, trades,
                   daily_return_pct, intraday_range_pct, volatility_7d
            FROM `{dataset}.fact_daily_metrics`
            WHERE open_date BETWEEN @start AND @end
              AND symbol IN UNNEST(@symbols)
            ORDER BY open_date""",
        job_config=bigquery.QueryJobConfig(query_parameters=[
            bigquery.ScalarQueryParameter("start", "DATE", start),
            bigquery.ScalarQueryParameter("end", "DATE", end),
            bigquery.ArrayQueryParameter("symbols", "STRING", list(symbols)),
        ]),
    )
    df = job.to_dataframe()
    df["open_date"] = pd.to_datetime(df["open_date"])
    df["month"] = df["open_date"].dt.to_period("M").astype(str)
    return df


# ------------------------------------------------------------------ sidebar
with st.sidebar:
    st.title("📈 Controls")
    project = st.secrets["gcp_service_account"]['project_id'] if "gcp_service_account" in st.secrets else os.environ.get("GCP_PROJECT")
    dataset = 'binance_analytics'
    fq_dataset = f"{project}.{dataset}" if project else dataset
    if not project:
        st.warning("Set the GCP project id to load data.")

    try:
        client = bq_client()
        all_symbols = load_symbols(client, fq_dataset)
    except Exception as e:
        st.error(f"Could not reach BigQuery: {e}")
        st.stop()

    today = pd.Timestamp.today().normalize()
    default_start = (today - pd.DateOffset(months=12)).date()
    date_range = st.date_input("Date range", value=(default_start, today.date()),
                               min_value=pd.Timestamp("2017-01-01").date())
    if len(date_range) != 2:
        st.info("Pick a start and end date.")
        st.stop()
    start, end = date_range

    symbols = st.multiselect("Symbols", all_symbols,
                             default=[s for s in ["BTCUSDT", "ETHUSDT"] if s in all_symbols],
                             max_selections=15)
    metric_label = st.selectbox("Chart metric", list(METRICS), index=0)
    chart_type = st.radio("Chart type", ["Line", "Area"], horizontal=True)
    ma_window = st.slider("Moving average (days)", 1, 30, 1,
                          help="Smooth the main chart with a rolling average")
    show_raw_table = st.checkbox("Show raw data table", value=False)
    st.divider()
    st.caption("Data: Binance public API · Pipeline: Airflow → GCS → BigQuery → dbt")

# ------------------------------------------------------------------ main
st.title("Binance Market Analytics")
st.caption("Daily OHLCV metrics for major USDT pairs · dbt model `fact_daily_metrics`")

if not symbols:
    st.warning("Select at least one symbol in the sidebar.")
    st.stop()

df = load_data(client, fq_dataset, symbols, start, end)
if df.empty:
    st.warning("No rows for this filter combination — widen the date range.")
    st.stop()

# ---- KPI row
latest = df.sort_values("open_date").groupby("symbol").tail(1)
k1, k2, k3, k4 = st.columns(4)
tot_volume = df["quote_volume"].sum()
avg_ret = df["daily_return_pct"].mean()
avg_vol = df["volatility_7d"].mean()
k1.metric("Symbols tracked", len(symbols), delta=f"{len(df):,} rows")
k2.metric("Avg daily return", f"{avg_ret:+.2f}%", delta=f"{df['daily_return_pct'].std():.2f}% σ")
k3.metric("Total quote volume", f"${tot_volume/1e9:,.1f}B")
k4.metric("Avg volatility (7d)", f"{avg_vol:.2f}%")

# ---- main time series with metric switcher + moving average
st.subheader(f"{metric_label} over time")
metric_col = METRICS[metric_label]
ts = df.pivot_table(index="open_date", columns="symbol", values=metric_col)
if ma_window > 1:
    ts_plot = ts.rolling(ma_window).mean()
else:
    ts_plot = ts

fig_ts = go.Figure()
for sym in ts_plot.columns:
    color = GOLD if sym == "BTCUSDT" else None
    fig_ts.add_trace(go.Scatter(
        x=ts_plot.index, y=ts_plot[sym], name=sym, mode="lines",
        fill="tozeroy" if chart_type == "Area" else None,
        opacity=0.9 if chart_type == "Area" else 1,
        line=dict(color=color, width=2),
    ))
fig_ts.update_layout(**PLOTLY_LAYOUT, height=420, yaxis_title=metric_label,
                     legend=dict(orientation="h", y=1.12, x=0))
st.plotly_chart(fig_ts, use_container_width=True)

# ---- row: categorical bar + heatmap
left, right = st.columns(2)
with left:
    st.subheader("Quote volume by pair")
    vol = df.groupby("symbol", as_index=False)["quote_volume"].sum().sort_values("quote_volume")
    fig_bar = px.bar(vol, x="quote_volume", y="symbol", orientation="h",
                     color="quote_volume", color_continuous_scale=["#3a2f10", GOLD],
                     labels={"quote_volume": "Quote volume (USDT)", "symbol": ""})
    fig_bar.update_layout(**PLOTLY_LAYOUT, height=380, coloraxis_showscale=False)
    st.plotly_chart(fig_bar, use_container_width=True)

with right:
    st.subheader("Volatility heatmap (symbol × month)")
    heat = df.pivot_table(index="symbol", columns="month", values="volatility_7d", aggfunc="mean")
    fig_heat = px.imshow(heat, color_continuous_scale="RdYlGn_r", aspect="auto",
                         labels=dict(color="volatility %"))
    fig_heat.update_layout(**PLOTLY_LAYOUT, height=380)
    st.plotly_chart(fig_heat, use_container_width=True)

# ---- monthly detail table with conditional formatting
st.subheader("Monthly breakdown")
monthly = (df.groupby(["symbol", "month"], as_index=False)
             .agg(avg_close=("closing_price", "mean"),
                  quote_volume=("quote_volume", "sum"),
                  avg_return=("daily_return_pct", "mean"),
                  avg_range=("intraday_range_pct", "mean")))
styled = (monthly.style
          .format({"avg_close": "{:,.2f}", "quote_volume": "{:,.0f}",
                   "avg_return": "{:+.2f}%", "avg_range": "{:.2f}%"})
          .bar(subset=["quote_volume"], color=GOLD)
          .map(lambda v: f"color: {GREEN}" if v >= 0 else f"color: {RED}",
               subset=["avg_return"]))
st.dataframe(styled, use_container_width=True, height=320)

# ---- top movers
st.subheader("🏆 Biggest single-day moves in range")
movers = (df.nlargest(8, "daily_return_pct")
          [["symbol", "open_date", "daily_return_pct", "quote_volume"]]
          .assign(open_date=lambda d: d["open_date"].dt.date))
movers.columns = ["Symbol", "Date", "Return %", "Quote volume"]
st.dataframe(movers, use_container_width=True, hide_index=True)

if show_raw_table:
    st.subheader("Raw rows")
    st.dataframe(df, use_container_width=True, height=300)
