import marimo

__generated_with = "0.24.2"
app = marimo.App(
    width="medium",
    app_title="Drivers: temperature, river flow, SBIWTP",
)


@app.cell
def _():
    import sys
    import marimo as mo
    import numpy as np
    import pandas as pd
    import altair as alt

    _here = str(mo.notebook_dir())
    if _here not in sys.path:
        sys.path.insert(0, _here)
    import tj_review_data as d

    alt.data_transformers.disable_max_rows()
    return alt, d, mo, np, pd


@app.cell
def _(mo):
    mo.md(r"""
    # Drivers: temperature, border streamflow and SBIWTP effluent

    **Questions for the planning session.**

    1. Is there a correlation between temperature and high H2S nights?
    2. Is there a correlation between Tijuana River flow at the border and high H2S nights?
    3. How do SBIWTP effluent flow and border flow relate to each other, and which
       (if either) relates to H2S?

    Data: the nightly summary (station × astronomical night) for the H2S side;
    the hourly model data for daily flows and temperature (the model data carries
    the IBWC border gauge and the SBIWTP daily effluent already aligned); the raw
    IBWC exports for the gauge-quality check.

    Temperature here is the OpenMeteo 2 m air temperature at each station, not a
    measurement. Border flow is IBWC gauge 11013300. SBIWTP is the plant's daily
    effluent in million US gallons per day (MGD); 1 MGD ≈ 0.044 m³/s.
    """)
    return


@app.cell
def _(d):
    hourly = d.hourly()
    nightly = d.nightly()
    return hourly, nightly


@app.cell
def _(hourly, pd):
    daily = (
        hourly[hourly["site_name"] == "NESTOR - BES"]
        .groupby("date")
        .agg(border_cms=("border_cms", "mean"), sbiwtp_mgd=("sbiwtp_flow_mgd", "mean"), temp_c=("temperature_2m", "mean"))
    )
    daily.index = pd.to_datetime(daily.index)
    daily["month"] = daily.index.month
    daily["year"] = daily.index.year
    daily["sbiwtp_cms"] = daily["sbiwtp_mgd"] * 0.043813
    return (daily,)


@app.cell
def _(mo):
    mo.md(r"""
    ## 1. Overview: rank correlation of each nightly driver with the night's peak

    Spearman ρ between the night's H2S peak (and, separately, its hours above
    30 ppb) and each candidate driver, per station. Sign matters as much as size.
    """)
    return


@app.cell
def _(d, nightly, pd):
    DRIVERS = {
        "temperature_2m_mean": "temperature (°C, night mean)",
        "flow_mean": "border flow (m³/s, night mean)",
        "sbiwtp_flow_mgd_mean": "SBIWTP effluent (MGD)",
        "tide_height_mean": "tide height (night mean)",
        "wind_speed_mean": "wind speed (night mean)",
        "wind_steadiness": "wind steadiness",
        "relative_humidity_2m_mean": "relative humidity",
        "surface_pressure_mean": "surface pressure",
        "stable_atm_hours": "stable-atmosphere hours",
        "prev_h2s_max": "previous night's peak",
    }
    rho_peak = d.spearman(nightly, "h2s_max", list(DRIVERS)).assign(target="night peak")
    rho_h30 = d.spearman(nightly, "hours_above_30", list(DRIVERS)).assign(target="hours above 30 ppb")
    rho = pd.concat([rho_peak, rho_h30])
    rho["driver"] = rho["driver"].map(DRIVERS)
    return (rho,)


@app.cell
def _(alt, d, mo, rho):
    _chart = (
        alt.Chart(rho)
        .mark_rect()
        .encode(
            y=alt.Y("driver:N", title=None, sort=list(dict.fromkeys(rho["driver"]))),
            x=alt.X("site_name:N", title=None, sort=d.STATIONS),
            color=alt.Color("rho:Q", title="Spearman ρ", scale=alt.Scale(scheme="blueorange", domain=[-0.6, 0.6])),
            tooltip=["site_name", "driver", "target", alt.Tooltip("rho:Q", format=".2f"), "n"],
        )
        .properties(width=170, height=300)
        .facet(column=alt.Column("target:N", title=None))
    )
    mo.ui.altair_chart(_chart)
    return


@app.cell
def _(mo):
    mo.md(r"""
    ## 2. Temperature

    The sign differs by station: at NESTOR and IB Civic Center the correlation
    with the night peak is *negative*; at San Ysidro it is *positive*. That is
    consistent with the exceedance season (March–May) being cool, not warm.
    The chart bins nights by mean temperature and shows how often the peak
    exceeded the chosen threshold.
    """)
    return


@app.cell
def _(mo):
    threshold = mo.ui.slider(5, 200, value=30, step=5, label="threshold (ppb)", show_value=True)
    threshold
    return (threshold,)


@app.cell
def _(alt, d, mo, nightly, pd, threshold):
    _t = threshold.value
    _n = nightly.dropna(subset=["temperature_2m_mean"]).copy()
    _n["temp_bin"] = pd.cut(_n["temperature_2m_mean"], [0, 10, 12, 14, 16, 18, 20, 22, 40], labels=["<10", "10–12", "12–14", "14–16", "16–18", "18–20", "20–22", ">22"])
    _b = _n.assign(pos=_n["h2s_max"] > _t).groupby(["site_name", "temp_bin"], observed=True).agg(nights=("pos", "size"), pos=("pos", "sum")).reset_index()
    _b["rate"] = _b["pos"] / _b["nights"]
    _chart = (
        alt.Chart(_b[_b["nights"] >= 10])
        .mark_line(point=alt.OverlayMarkDef(size=50), strokeWidth=2)
        .encode(
            x=alt.X("temp_bin:N", title="night mean air temperature (°C)", sort=list(_b["temp_bin"].cat.categories)),
            y=alt.Y("rate:Q", title=f"share of nights with peak > {_t} ppb", axis=alt.Axis(format=".0%")),
            color=alt.Color("site_name:N", sort=d.STATIONS, scale=alt.Scale(domain=d.STATIONS, range=[d.STATION_COLORS[s] for s in d.STATIONS]), legend=alt.Legend(title="station")),
            tooltip=["site_name", "temp_bin", "nights", "pos", alt.Tooltip("rate:Q", format=".1%")],
        )
        .properties(width=520, height=240, title=f"Exceedance rate by temperature (bins with ≥10 nights)")
    )
    mo.ui.altair_chart(_chart)
    return


@app.cell
def _(mo, nightly, pd, threshold):
    _t = threshold.value
    _rows = []
    for _site, _g in nightly.dropna(subset=["temperature_2m_mean"]).groupby("site_name", observed=True):
        _g = _g.copy()
        _g["temp_anom"] = _g["temperature_2m_mean"] - _g.groupby("month")["temperature_2m_mean"].transform("mean")
        _g["pos"] = _g["h2s_max"] > _t
        _warm = _g["temp_anom"] > 0
        _rows.append(
            {
                "station": _site,
                "ρ(peak, temperature)": _g[["h2s_max", "temperature_2m_mean"]].corr(method="spearman").iloc[0, 1],
                "ρ(peak, temperature anomaly within month)": _g[["h2s_max", "temp_anom"]].corr(method="spearman").iloc[0, 1],
                f"P(>{_t} | warmer than month mean)": _g.loc[_warm, "pos"].mean(),
                f"P(>{_t} | cooler than month mean)": _g.loc[~_warm, "pos"].mean(),
            }
        )
    mo.vstack(
        [
            mo.md("Removing the seasonal cycle: correlation with the temperature *anomaly within the month* isolates whether a warm night, for the time of year, is worse."),
            mo.ui.table(pd.DataFrame(_rows).round(3), selection=None),
        ]
    )
    return


@app.cell
def _(mo):
    mo.md(r"""
    ## 3. Border streamflow

    ### 3a. Is the gauge trustworthy?

    Before using flow as a predictor: the IBWC record has long stretches at
    exactly the same value and dry-season medians that differ by an order of
    magnitude between years. Monthly medians by year, from the raw exports.
    """)
    return


@app.cell
def _(alt, d, mo):
    _b = d.border_flow()
    _b["month"] = _b["time"].dt.month
    _b["year"] = _b["time"].dt.year
    _m = _b.groupby(["year", "month"])["border_cms"].median().reset_index()
    _m["border_plot"] = _m["border_cms"].clip(lower=0.01)
    _chart = (
        alt.Chart(_m)
        .mark_line(point=alt.OverlayMarkDef(size=40), strokeWidth=2)
        .encode(
            x=alt.X("month:O", title="month"),
            y=alt.Y("border_plot:Q", title="monthly median flow (m³/s, log)", scale=alt.Scale(type="log")),
            color=alt.Color("year:O", scale=alt.Scale(scheme="blues"), legend=alt.Legend(title="year")),
            tooltip=["year", "month", alt.Tooltip("border_cms:Q", format=".2f")],
        )
        .properties(width=520, height=240, title="IBWC 11013300 monthly median flow by year")
    )
    mo.ui.altair_chart(_chart)
    return


@app.cell
def _(d, mo, np):
    _b = d.border_flow().dropna()
    _v = _b["border_cms"].round(3).to_numpy()
    _breaks = np.r_[True, _v[1:] != _v[:-1]]
    _run = np.cumsum(_breaks)
    _r = _b.assign(run=_run).groupby("run").agg(value=("border_cms", "first"), hours=("border_cms", "size"), start=("time", "first"))
    _r = _r[_r["value"] > 0].sort_values("hours", ascending=False).head(10).reset_index(drop=True)
    _r["start"] = _r["start"].dt.strftime("%Y-%m-%d %H:%M")
    mo.vstack([mo.md("Longest runs of an unchanging non-zero hourly value (a gauge that is stuck, or a value that has been filled):"), mo.ui.table(_r, selection=None)])
    return


@app.cell
def _(mo):
    mo.md(r"""
    ### 3b. Flow against exceedance

    Nights binned by the night's mean border flow. Very low flows are the dry
    season; flows above ~5 m³/s are storms.
    """)
    return


@app.cell
def _(alt, d, mo, nightly, pd, threshold):
    _t = threshold.value
    _n = nightly.dropna(subset=["flow_mean"]).copy()
    _n["flow_bin"] = pd.cut(_n["flow_mean"], [-0.01, 0.1, 0.5, 1, 2, 3, 5, 10, 1000], labels=["<0.1", "0.1–0.5", "0.5–1", "1–2", "2–3", "3–5", "5–10", ">10"])
    _b = _n.assign(pos=_n["h2s_max"] > _t).groupby(["site_name", "flow_bin"], observed=True).agg(nights=("pos", "size"), pos=("pos", "sum")).reset_index()
    _b["rate"] = _b["pos"] / _b["nights"]
    _chart = (
        alt.Chart(_b[_b["nights"] >= 10])
        .mark_line(point=alt.OverlayMarkDef(size=50), strokeWidth=2)
        .encode(
            x=alt.X("flow_bin:N", title="night mean border flow (m³/s)", sort=list(_b["flow_bin"].cat.categories)),
            y=alt.Y("rate:Q", title=f"share of nights with peak > {_t} ppb", axis=alt.Axis(format=".0%")),
            color=alt.Color("site_name:N", sort=d.STATIONS, scale=alt.Scale(domain=d.STATIONS, range=[d.STATION_COLORS[s] for s in d.STATIONS]), legend=alt.Legend(title="station")),
            tooltip=["site_name", "flow_bin", "nights", "pos", alt.Tooltip("rate:Q", format=".1%")],
        )
        .properties(width=520, height=240, title="Exceedance rate by border flow (bins with ≥10 nights)")
    )
    mo.ui.altair_chart(_chart)
    return


@app.cell
def _(mo):
    mo.md(r"""
    ## 4. SBIWTP effluent

    ### 4a. SBIWTP against exceedance

    Earlier work found that *higher* plant throughput goes with *lower* H2S
    (the plant treating more means less raw sewage in the channel). Nights
    binned by the day's effluent.
    """)
    return


@app.cell
def _(alt, d, mo, nightly, pd, threshold):
    _t = threshold.value
    _n = nightly.dropna(subset=["sbiwtp_flow_mgd_mean"]).copy()
    _n["mgd_bin"] = pd.cut(_n["sbiwtp_flow_mgd_mean"], [0, 18, 21, 24, 27, 30, 33, 100], labels=["<18", "18–21", "21–24", "24–27", "27–30", "30–33", ">33"])
    _b = _n.assign(pos=_n["h2s_max"] > _t).groupby(["site_name", "mgd_bin"], observed=True).agg(nights=("pos", "size"), pos=("pos", "sum")).reset_index()
    _b["rate"] = _b["pos"] / _b["nights"]
    _chart = (
        alt.Chart(_b[_b["nights"] >= 10])
        .mark_line(point=alt.OverlayMarkDef(size=50), strokeWidth=2)
        .encode(
            x=alt.X("mgd_bin:N", title="SBIWTP effluent (MGD)", sort=list(_b["mgd_bin"].cat.categories)),
            y=alt.Y("rate:Q", title=f"share of nights with peak > {_t} ppb", axis=alt.Axis(format=".0%")),
            color=alt.Color("site_name:N", sort=d.STATIONS, scale=alt.Scale(domain=d.STATIONS, range=[d.STATION_COLORS[s] for s in d.STATIONS]), legend=alt.Legend(title="station")),
            tooltip=["site_name", "mgd_bin", "nights", "pos", alt.Tooltip("rate:Q", format=".1%")],
        )
        .properties(width=520, height=240, title="Exceedance rate by SBIWTP effluent (bins with ≥10 nights)")
    )
    mo.ui.altair_chart(_chart)
    return


@app.cell
def _(mo):
    mo.md(r"""
    ### 4b. SBIWTP effluent against border flow

    The two flows measure different things: the gauge is what reaches the
    river at the border, the plant is what is diverted and treated. Their
    relationship changes sign with the season.
    """)
    return


@app.cell
def _(alt, daily, mo):
    _d = daily.dropna(subset=["border_cms", "sbiwtp_cms"]).copy()
    _d["border_plot"] = _d["border_cms"].clip(lower=0.01)
    _d["season"] = _d["month"].map(lambda m: "wet (Nov–Apr)" if m in (11, 12, 1, 2, 3, 4) else "dry (May–Oct)")
    _chart = (
        alt.Chart(_d.reset_index(names="date"))
        .mark_circle(size=22, opacity=0.5)
        .encode(
            x=alt.X("sbiwtp_cms:Q", title="SBIWTP effluent (m³/s)"),
            y=alt.Y("border_plot:Q", title="border flow (m³/s, log)", scale=alt.Scale(type="log")),
            color=alt.Color("season:N", scale=alt.Scale(domain=["wet (Nov–Apr)", "dry (May–Oct)"], range=["#2a78d6", "#eb6834"]), legend=alt.Legend(title=None, orient="bottom")),
            tooltip=[alt.Tooltip("date:T"), alt.Tooltip("sbiwtp_cms:Q", format=".2f"), alt.Tooltip("border_cms:Q", format=".2f")],
        )
        .properties(width=520, height=280, title="Daily SBIWTP effluent vs border flow")
    )
    mo.ui.altair_chart(_chart)
    return


@app.cell
def _(daily, mo, pd):
    _d = daily.dropna(subset=["border_cms", "sbiwtp_cms"])
    _overall = _d[["border_cms", "sbiwtp_cms"]].corr(method="spearman").iloc[0, 1]
    _by_month = _d.groupby("month").apply(lambda g: g[["border_cms", "sbiwtp_cms"]].corr(method="spearman").iloc[0, 1] if len(g) > 10 else float("nan"), include_groups=False)
    _lags = {k: _d["border_cms"].corr(_d["sbiwtp_cms"].shift(-k), method="spearman") for k in [-7, -3, -1, 0, 1, 3, 7]}
    mo.vstack(
        [
            mo.md(f"Daily Spearman ρ between border flow and SBIWTP effluent, all days: **{_overall:.2f}** (n = {len(_d):,})."),
            mo.md("By calendar month:"),
            mo.ui.table(_by_month.round(2).rename("ρ").reset_index(), selection=None),
            mo.md("By lag (positive k: SBIWTP k days *after* the border reading):"),
            mo.ui.table(pd.Series(_lags, name="ρ").round(2).rename_axis("lag (days)").reset_index(), selection=None),
        ]
    )
    return


@app.cell
def _(mo):
    mo.md(r"""
    ### 4c. Low treatment × warm night

    The combination flagged in the SBIWTP incorporation plan, re-checked on the
    current record and on nights rather than calendar days.
    """)
    return


@app.cell
def _(mo, nightly, pd, threshold):
    _t = threshold.value
    _rows = []
    for _site, _g in nightly.dropna(subset=["sbiwtp_flow_mgd_mean", "temperature_2m_mean"]).groupby("site_name", observed=True):
        _low = _g["sbiwtp_flow_mgd_mean"] < _g["sbiwtp_flow_mgd_mean"].quantile(0.25)
        _warm = _g["temperature_2m_mean"] > _g["temperature_2m_mean"].median()
        for _lname, _lm in [("low SBIWTP (bottom quartile)", _low), ("normal / high SBIWTP", ~_low)]:
            for _wname, _wm in [("warm night", _warm), ("cool night", ~_warm)]:
                _sel = _g[_lm & _wm]
                _rows.append({"station": _site, "treatment": _lname, "temperature": _wname, "nights": len(_sel), "mean peak (ppb)": _sel["h2s_max"].mean(), f"P(peak > {_t})": (_sel["h2s_max"] > _t).mean()})
    mo.ui.table(pd.DataFrame(_rows).round(2), selection=None, page_size=12)
    return


@app.cell
def _(mo):
    mo.md(r"""
    ## 5. Points for the session

    - **Temperature is not a simple "warmer is worse".** Across the year the
      correlation is negative at two of three stations because the bad season
      is spring. Within a month, the picture is weaker still. Temperature belongs
      in the model as one input among several, not as the headline driver.
    - **Border flow is a weak and noisy predictor**, and the gauge record has
      stretches that look filled or stuck. Any flow feature needs a quality flag
      alongside it.
    - **SBIWTP effluent is the strongest single driver in the set**, and it is
      negative: less treatment, more H2S. Its relationship with the border gauge
      flips sign between wet and dry seasons, so the two are not substitutes.
    - **Last night's peak beats every physical driver.** Persistence should be
      the baseline the forecast is scored against, and a feature the forecast
      model is allowed to use *at the lead time it is actually available*.
    """)
    return


if __name__ == "__main__":
    app.run()
