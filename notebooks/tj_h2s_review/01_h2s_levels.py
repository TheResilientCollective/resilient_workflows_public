import marimo

__generated_with = "0.24.2"
app = marimo.App(width="medium", app_title="H2S levels: 30 and 100 ppb")


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
    return alt, d, mo, pd


@app.cell
def _(mo):
    mo.md(r"""
    # H2S levels: how much data is there at 30 ppb and above?

    **Question for the planning session.** APCD asked us to focus on predicting
    H2S *levels*, and specifically to do better at 30 ppb and above. Before
    choosing a model we need to know how many 30 ppb and 100 ppb events the
    record actually contains, at which stations, in which seasons, and how
    long they last.

    Data: `h2sforecast/modeldata_h2s_nofill` (hourly, measured values only) and
    `h2sforecast/h2s_nightly_summary_with_complaints` (one row per station per
    astronomical night). Both are the production datasets.
    """)
    return


@app.cell
def _(d):
    hourly = d.hourly()
    nightly = d.nightly()
    return hourly, nightly


@app.cell
def _(hourly, mo, nightly):
    mo.md(
        f"""
        ## 1. Record inventory

        {len(hourly):,} measured station-hours from {hourly.time.min():%Y-%m-%d} to
        {hourly.time.max():%Y-%m-%d}; {len(nightly):,} station-nights with at least
        half their hours measured.
        """
    )
    return


@app.cell
def _(hourly, mo):
    _inv = (
        hourly.groupby(["site_name", "year"], observed=True)
        .agg(measured_hours=("H2S", "size"), first=("time", "min"), last=("time", "max"), max_ppb=("H2S", "max"))
        .reset_index()
    )
    _inv["first"] = _inv["first"].dt.date
    _inv["last"] = _inv["last"].dt.date
    mo.ui.table(_inv, selection=None, page_size=12)
    return


@app.cell
def _(mo):
    mo.md(r"""
    ## 2. Exceedance hours by threshold

    Hours above each threshold, per station. The deployed classifiers are trained
    at 5 and 10 ppb; the APCD ask is 30 ppb.
    """)
    return


@app.cell
def _(d, hourly, mo, pd):
    _rows = []
    for _site, _g in hourly.groupby("site_name", observed=True):
        _r = {"station": _site, "measured hours": len(_g)}
        for _t in d.THRESHOLDS:
            _r[f"> {_t} ppb"] = int((_g["H2S"] > _t).sum())
        _rows.append(_r)
    exceed_table = pd.DataFrame(_rows)
    mo.ui.table(exceed_table, selection=None)
    return (exceed_table,)


@app.cell
def _(alt, d, exceed_table, mo):
    _long = exceed_table.melt(id_vars=["station", "measured hours"], var_name="threshold", value_name="hours")
    _long["threshold_ppb"] = _long["threshold"].str.extract(r"(\d+)").astype(int)
    _long["share"] = _long["hours"] / _long["measured hours"]
    _chart = (
        alt.Chart(_long)
        .mark_line(point=alt.OverlayMarkDef(size=60), strokeWidth=2)
        .encode(
            x=alt.X("threshold_ppb:O", title="threshold (ppb)"),
            y=alt.Y("share:Q", title="share of measured hours above threshold", scale=alt.Scale(type="symlog", constant=0.001), axis=alt.Axis(format=".2%")),
            color=alt.Color("station:N", sort=d.STATIONS, scale=alt.Scale(domain=d.STATIONS, range=[d.STATION_COLORS[s] for s in d.STATIONS])),
            tooltip=["station", "threshold", "hours", alt.Tooltip("share:Q", format=".2%")],
        )
        .properties(width=520, height=260, title="How rare each threshold is (symlog y-axis)")
    )
    mo.ui.altair_chart(_chart)
    return


@app.cell
def _(mo):
    mo.md(r"""
    ## 3. Nights above threshold

    A warning product is really a *nightly* product: events sit inside one
    astronomical night. Counting nights, not hours, is what tells us how many
    independent positive examples a classifier would see.
    """)
    return


@app.cell
def _(d, mo, nightly, pd):
    _rows = []
    for _site, _g in nightly.groupby("site_name", observed=True):
        _r = {"station": _site, "nights": len(_g)}
        for _t in d.THRESHOLDS:
            _r[f"peak > {_t}"] = int((_g["h2s_max"] > _t).sum())
        _rows.append(_r)
    nights_table = pd.DataFrame(_rows)
    mo.ui.table(nights_table, selection=None)
    return


@app.cell
def _(mo):
    threshold = mo.ui.slider(5, 300, value=30, step=5, label="threshold (ppb)", show_value=True)
    threshold
    return (threshold,)


@app.cell
def _(mo, nightly, threshold):
    _t = threshold.value
    _pos = nightly[nightly["h2s_max"] > _t]
    _by_site_year = _pos.groupby(["site_name", "astro_year"], observed=True).size().unstack(fill_value=0)
    _by_site_year.columns = [str(c) for c in _by_site_year.columns]
    mo.vstack(
        [
            mo.md(f"**Nights with peak > {_t} ppb:** {len(_pos)} of {len(nightly)} ({len(_pos) / len(nightly):.1%})"),
            mo.md("Per station and year (a walk-forward evaluation needs positives in *every* year):"),
            mo.ui.table(_by_site_year.reset_index(), selection=None),
        ]
    )
    return


@app.cell
def _(alt, d, mo, nightly, threshold):
    _t = threshold.value
    _m = nightly.assign(pos=(nightly["h2s_max"] > _t)).groupby(["site_name", "month"], observed=True).agg(nights=("pos", "size"), pos=("pos", "sum")).reset_index()
    _m["rate"] = _m["pos"] / _m["nights"]
    _chart = (
        alt.Chart(_m)
        .mark_bar(cornerRadiusTopLeft=3, cornerRadiusTopRight=3)
        .encode(
            x=alt.X("month:O", title="month"),
            y=alt.Y("rate:Q", title=f"share of nights with peak > {_t} ppb", axis=alt.Axis(format=".0%")),
            color=alt.Color("site_name:N", sort=d.STATIONS, scale=alt.Scale(domain=d.STATIONS, range=[d.STATION_COLORS[s] for s in d.STATIONS]), legend=alt.Legend(title="station")),
            xOffset="site_name:N",
            tooltip=["site_name", "month", "nights", "pos", alt.Tooltip("rate:Q", format=".1%")],
        )
        .properties(width=560, height=240, title=f"Seasonality of nights above {_t} ppb")
    )
    mo.ui.altair_chart(_chart)
    return


@app.cell
def _(mo):
    mo.md(r"""
    ## 4. When in the day do high hours occur?

    Month × hour heat map of exceedance hours. Pick the station and threshold;
    the diagonal shape across the year is the night lengthening and shortening.
    """)
    return


@app.cell
def _(d, mo):
    station_pick = mo.ui.dropdown(d.STATIONS, value="NESTOR - BES", label="station")
    station_pick
    return (station_pick,)


@app.cell
def _(alt, hourly, mo, station_pick, threshold):
    _t = threshold.value
    _g = hourly[hourly["site_name"] == station_pick.value]
    _h = _g.assign(pos=_g["H2S"] > _t).groupby(["month", "hour"]).agg(hours=("pos", "size"), pos=("pos", "sum")).reset_index()
    _h["rate"] = _h["pos"] / _h["hours"]
    _chart = (
        alt.Chart(_h)
        .mark_rect()
        .encode(
            x=alt.X("hour:O", title="hour of day (local)"),
            y=alt.Y("month:O", title="month"),
            color=alt.Color("rate:Q", title=f"share > {_t} ppb", scale=alt.Scale(scheme="blues")),
            tooltip=["month", "hour", "hours", "pos", alt.Tooltip("rate:Q", format=".1%")],
        )
        .properties(width=560, height=260, title=f"{station_pick.value}: share of hours above {_t} ppb")
    )
    mo.ui.altair_chart(_chart)
    return


@app.cell
def _(mo):
    mo.md(r"""
    ## 5. Event structure: how long does an exceedance last?

    Runs of consecutive measured hours above the threshold. Short runs mean a
    single-hour miss is a whole event missed; long runs mean an event that has
    started can be nowcast from its own onset.
    """)
    return


@app.cell
def _(d, hourly, mo, threshold):
    runs = d.runs_above(hourly, threshold.value)
    _summary = runs.groupby("site_name", observed=True)["hours"].describe()[["count", "mean", "50%", "75%", "max"]].round(1).reset_index()
    _summary.columns = ["station", "runs", "mean hours", "median hours", "p75 hours", "longest"]
    mo.ui.table(_summary, selection=None)
    return (runs,)


@app.cell
def _(alt, d, mo, runs, threshold):
    _chart = (
        alt.Chart(runs)
        .mark_bar(cornerRadiusTopLeft=3, cornerRadiusTopRight=3)
        .encode(
            x=alt.X("hours:O", title="run length (consecutive hours)"),
            y=alt.Y("count():Q", title="runs"),
            color=alt.Color("site_name:N", sort=d.STATIONS, scale=alt.Scale(domain=d.STATIONS, range=[d.STATION_COLORS[s] for s in d.STATIONS]), legend=alt.Legend(title="station")),
            xOffset="site_name:N",
            tooltip=["site_name", "hours", "count()"],
        )
        .properties(width=560, height=220, title=f"Run lengths above {threshold.value} ppb")
    )
    mo.ui.altair_chart(_chart)
    return


@app.cell
def _(mo):
    mo.md(r"""
    ## 6. Night-to-night persistence

    If tonight's peak is above the threshold, how often is tomorrow's? This is
    the skill of the simplest possible forecast ("same as last night") and is
    the bar any model has to clear.
    """)
    return


@app.cell
def _(mo, nightly, pd, threshold):
    _t = threshold.value
    _rows = []
    for _site, _g in nightly.groupby("site_name", observed=True):
        _g = _g.dropna(subset=["prev_h2s_max"])
        _cur = _g["h2s_max"] > _t
        _prev = _g["prev_h2s_max"] > _t
        _rows.append(
            {
                "station": _site,
                "base rate": _cur.mean(),
                "P(>t | last night >t)": (_cur & _prev).sum() / max(_prev.sum(), 1),
                "P(>t | last night ≤t)": (_cur & ~_prev).sum() / max((~_prev).sum(), 1),
                "nights after a >t night": int(_prev.sum()),
            }
        )
    persistence = pd.DataFrame(_rows)
    mo.ui.table(persistence.round(3), selection=None)
    return


@app.cell
def _(mo):
    mo.md(r"""
    ## 7. What this means for a model

    Rough rule: a tree-based classifier needs on the order of 50–100 positive
    *nights* per station to give a calibrated probability, and a walk-forward
    evaluation needs positives in every test block. Use the slider above to see
    where the record runs out.

    Things to decide in the session:

    - **Target definition.** Hourly value, nightly peak, or nightly hours above
      threshold? The nightly peak is the quantity APCD acts on, and it turns a
      rare hourly class into a less rare nightly class.
    - **Station scope for 100 ppb.** Only NESTOR - BES has more than a handful
      of nights above 100 ppb; a 100 ppb product is a NESTOR product.
    - **Training filter.** `train_models_auto.py` drops rows with H2S above
      500 ppb before training. The largest events in the record are exactly the
      ones being removed.
    - **Which classifier.** Today's deployed probabilities are for 5 and 10 ppb;
      the published skill report shows recall of 30 ppb events at every lead time
      is zero. A 30 ppb classifier does not exist yet.
    """)
    return


if __name__ == "__main__":
    app.run()
