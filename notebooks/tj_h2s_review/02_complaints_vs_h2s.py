import marimo

__generated_with = "0.24.2"
app = marimo.App(width="medium", app_title="Odour complaints vs H2S")


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
    # At what H2S levels do odour complaints occur?

    **Question for the planning session.** APCD's public complaint record is the
    only ground truth we have for *nuisance*, as opposed to concentration. If
    complaints start well below 30 ppb the product should say so; if they climb
    steeply above it, 30 ppb is the right line to draw.

    Data: `complaints/sd_complaints` (SDAPCD public complaints, real time of day)
    joined to the hourly station record and the nightly summary.

    **A caveat that shapes everything below.** The complaint dataset locates a
    complaint at APCD's *source adjacent location*, not where the complainant
    was. Most Tijuana River odour reports are filed against one intersection,
    `Caspian Way and N McCoy Trail`, so the complaint location cannot be matched
    to a monitoring station. Every complaint-to-station join here is by *time
    only*; the station is a choice, not a measurement.
    """)
    return


@app.cell
def _(d):
    hourly = d.hourly()
    nightly = d.nightly()
    complaints = d.complaints()
    return complaints, hourly, nightly


@app.cell
def _(complaints, mo):
    _by_nature = complaints["nature_of_complaint"].value_counts().head(8).rename_axis("nature").reset_index(name="complaints")
    _odor = complaints[complaints["is_odor"]]
    mo.vstack(
        [
            mo.md(
                f"""
                ## 1. Inventory

                {len(complaints):,} complaints from {complaints.dt.min():%Y-%m-%d} to
                {complaints.dt.max():%Y-%m-%d}. {int(complaints.is_odor.sum()):,} mention odour;
                of those {int((_odor.in_trv).sum()):,} are geocoded inside the Tijuana River
                Valley box and {int(_odor.is_caspian.sum()):,} carry the Caspian Way location.
                """
            ),
            mo.ui.table(_by_nature, selection=None),
        ]
    )
    return


@app.cell
def _(alt, complaints, mo):
    _odor = complaints[complaints["is_odor"] & complaints["in_trv"]]
    _yr = _odor.groupby(["year", "is_caspian"]).size().reset_index(name="complaints")
    _yr["location"] = _yr["is_caspian"].map({True: "Caspian Way and N McCoy Trail", False: "other TRV location"})
    _chart = (
        alt.Chart(_yr)
        .mark_bar(cornerRadiusTopLeft=3, cornerRadiusTopRight=3)
        .encode(
            x=alt.X("year:O", title="year"),
            y=alt.Y("complaints:Q"),
            color=alt.Color("location:N", scale=alt.Scale(domain=["Caspian Way and N McCoy Trail", "other TRV location"], range=["#2a78d6", "#eb6834"])),
            xOffset="location:N",
            tooltip=["year", "location", "complaints"],
        )
        .properties(width=480, height=220, title="Tijuana River Valley odour complaints by year")
    )
    mo.ui.altair_chart(_chart)
    return


@app.cell
def _(mo):
    mo.md(r"""
    ## 2. Time of day: complaints arrive in the morning, H2S peaks at night

    Complaints are stamped when they are *received*. The evening rise tracks the
    H2S rise, but the largest block of complaints is 06:00–09:00, hours after
    the night peak. A complaint at 07:30 is evidence about the night before.
    """)
    return


@app.cell
def _(alt, complaints, hourly, mo, pd):
    _odor = complaints[complaints["is_odor"] & complaints["in_trv"]]
    _c = _odor.groupby("hour").size().rename("value").reset_index().assign(series="odour complaints received (share of all)")
    _c["value"] = _c["value"] / _c["value"].sum()
    _n = hourly[hourly["site_name"] == "NESTOR - BES"]
    _h = (_n["H2S"] > 30).groupby(_n["hour"]).mean().rename("value").reset_index().assign(series="share of NESTOR hours > 30 ppb")
    _both = pd.concat([_c, _h])
    _chart = (
        alt.Chart(_both)
        .mark_line(point=alt.OverlayMarkDef(size=40), strokeWidth=2)
        .encode(
            x=alt.X("hour:O", title="hour of day (local)"),
            y=alt.Y("value:Q", title="share", axis=alt.Axis(format=".0%")),
            color=alt.Color("series:N", scale=alt.Scale(range=["#2a78d6", "#eb6834"]), legend=alt.Legend(title=None, orient="bottom")),
            tooltip=["series", "hour", alt.Tooltip("value:Q", format=".1%")],
        )
        .properties(width=560, height=240, title="Diurnal pattern of complaints and of high H2S")
    )
    mo.ui.altair_chart(_chart)
    return


@app.cell
def _(mo):
    mo.md(r"""
    ## 3. H2S at the time of a complaint

    Each Tijuana River Valley odour complaint is matched to the chosen station's
    H2S in the hour it was received, and to the highest reading in the previous
    *N* hours (to allow for the reporting delay). The curve is the distribution
    of those readings.
    """)
    return


@app.cell
def _(d, mo):
    station_pick = mo.ui.dropdown(d.STATIONS, value="NESTOR - BES", label="station")
    lookback = mo.ui.slider(1, 12, value=6, label="look-back window (hours)", show_value=True)
    mo.hstack([station_pick, lookback])
    return lookback, station_pick


@app.cell
def _(complaints, hourly, lookback, station_pick):
    _s = hourly[hourly["site_name"] == station_pick.value].set_index("time").sort_index()
    _s = _s[~_s.index.duplicated()]
    _roll = _s["H2S"].rolling(f"{lookback.value}h", min_periods=1).max().rename("h2s_recent_max")
    _odor = complaints[complaints["is_odor"] & complaints["in_trv"]].copy()
    joined = _odor.merge(_s[["H2S"]].rename(columns={"H2S": "h2s_at_hour"}), left_on="hour_floor", right_index=True, how="left")
    joined = joined.merge(_roll, left_on="hour_floor", right_index=True, how="left")
    joined = joined.dropna(subset=["h2s_at_hour"])
    return (joined,)


@app.cell
def _(joined, lookback, mo, station_pick):
    _q = joined[["h2s_at_hour", "h2s_recent_max"]].quantile([0.1, 0.25, 0.5, 0.75, 0.9]).round(1)
    _q.index = [f"p{int(i*100)}" for i in _q.index]
    _q.columns = ["H2S in the hour received (ppb)", f"max H2S in previous {lookback.value} h (ppb)"]
    mo.vstack(
        [
            mo.md(f"{len(joined):,} TRV odour complaints have a {station_pick.value} reading in the hour they were received."),
            mo.ui.table(_q.reset_index(names="quantile"), selection=None),
        ]
    )
    return


@app.cell
def _(alt, joined, lookback, mo, pd):
    _a = joined[["h2s_at_hour"]].rename(columns={"h2s_at_hour": "ppb"}).assign(series="in the hour received")
    _b = joined[["h2s_recent_max"]].rename(columns={"h2s_recent_max": "ppb"}).assign(series=f"max of previous {lookback.value} h")
    _both = pd.concat([_a, _b]).dropna()
    _both["ppb"] = _both["ppb"].clip(lower=0.1)
    _chart = (
        alt.Chart(_both)
        .transform_window(ecdf="cume_dist()", sort=[{"field": "ppb"}], groupby=["series"])
        .mark_line(strokeWidth=2, interpolate="step-after")
        .encode(
            x=alt.X("ppb:Q", title="H2S (ppb, log scale)", scale=alt.Scale(type="log", domain=[0.1, 1000])),
            y=alt.Y("ecdf:Q", title="share of complaints at or below", axis=alt.Axis(format=".0%")),
            color=alt.Color("series:N", scale=alt.Scale(range=["#2a78d6", "#eb6834"]), legend=alt.Legend(title=None, orient="bottom")),
        )
        .properties(width=560, height=260, title="Cumulative distribution of H2S when odour complaints are received")
    )
    _rules = alt.Chart(pd.DataFrame({"ppb": [5, 30, 100]})).mark_rule(color="#898781", strokeDash=[4, 4]).encode(x="ppb:Q")
    mo.ui.altair_chart(_chart + _rules)
    return


@app.cell
def _(mo):
    mo.md(r"""
    ## 4. Complaint *rate* by H2S level

    The distribution above is dominated by how common low readings are. The
    better question is: per hour spent at a given H2S level, how many complaints
    arrive? This normalises by exposure.
    """)
    return


@app.cell
def _(hourly, joined, mo, pd, station_pick):
    _bins = [-0.01, 1, 5, 10, 30, 50, 100, 200, 10_000]
    _labels = ["≤1", "1–5", "5–10", "10–30", "30–50", "50–100", "100–200", ">200"]
    _s = hourly[hourly["site_name"] == station_pick.value]
    _hours = pd.cut(_s["H2S"], _bins, labels=_labels).value_counts().sort_index()
    _cmpl = pd.cut(joined["h2s_at_hour"], _bins, labels=_labels).value_counts().sort_index()
    rate = pd.DataFrame({"H2S bin (ppb)": _labels, "measured hours": _hours.values, "complaints": _cmpl.values})
    rate["complaints per 100 hours"] = (100 * rate["complaints"] / rate["measured hours"]).round(1)
    mo.ui.table(rate, selection=None)
    return (rate,)


@app.cell
def _(alt, mo, rate, station_pick):
    _chart = (
        alt.Chart(rate)
        .mark_bar(color="#2a78d6", cornerRadiusTopLeft=3, cornerRadiusTopRight=3)
        .encode(
            x=alt.X("H2S bin (ppb):N", sort=list(rate["H2S bin (ppb)"]), title="H2S in the hour the complaint was received (ppb)"),
            y=alt.Y("complaints per 100 hours:Q"),
            tooltip=["H2S bin (ppb)", "measured hours", "complaints", "complaints per 100 hours"],
        )
        .properties(width=520, height=240, title=f"Odour complaints per 100 measured hours, {station_pick.value}")
    )
    mo.ui.altair_chart(_chart)
    return


@app.cell
def _(mo):
    mo.md(r"""
    ## 5. Nightly view: complaints against the night's peak

    One point per station-night. Complaint counts are night-wide (the same count
    is attached to every station for that night), so the three panels differ
    only in the H2S axis.
    """)
    return


@app.cell
def _(alt, d, mo, nightly):
    _n = nightly.copy()
    _n["h2s_max_plot"] = _n["h2s_max"].clip(lower=0.1)
    _chart = (
        alt.Chart(_n)
        .mark_circle(size=28, opacity=0.5)
        .encode(
            x=alt.X("h2s_max_plot:Q", title="night peak H2S (ppb, log)", scale=alt.Scale(type="log", domain=[0.1, 1000])),
            y=alt.Y("complaints_total:Q", title="complaints in that astronomical day"),
            color=alt.Color("site_name:N", sort=d.STATIONS, scale=alt.Scale(domain=d.STATIONS, range=[d.STATION_COLORS[s] for s in d.STATIONS]), legend=None),
            tooltip=["site_name", "astro_day_date", "h2s_max", "complaints_total", "complaints_at_night"],
        )
        .properties(width=180, height=200)
        .facet(column=alt.Column("site_name:N", sort=d.STATIONS, title=None))
    )
    mo.ui.altair_chart(_chart)
    return


@app.cell
def _(d, mo, nightly):
    _rho = d.spearman(nightly, "complaints_total", ["h2s_max", "h2s_p95", "hours_above_5", "hours_above_30"])
    _wide = _rho.pivot(index="site_name", columns="driver", values="rho").round(2).reset_index()
    mo.vstack([mo.md("Spearman rank correlation between nightly complaint count and the night's H2S statistics:"), mo.ui.table(_wide, selection=None)])
    return


@app.cell
def _(mo):
    mo.md(r"""
    ## 6. Threshold view: what a warning at level *t* would have meant

    For a candidate threshold, two conditional rates: the share of nights
    above *t* that produced at least *k* complaints, and the share of nights
    with at least *k* complaints on which the peak was above *t*.
    """)
    return


@app.cell
def _(mo):
    min_complaints = mo.ui.slider(1, 20, value=5, label="complaints that count as a 'complaint night'", show_value=True)
    min_complaints
    return (min_complaints,)


@app.cell
def _(min_complaints, mo, nightly, pd):
    _rows = []
    for _site, _g in nightly.groupby("site_name", observed=True):
        _cn = _g["complaints_total"] >= min_complaints.value
        for _t in [5, 10, 30, 50, 100]:
            _hi = _g["h2s_max"] > _t
            _rows.append(
                {
                    "station": _site,
                    "threshold": _t,
                    "nights above t": int(_hi.sum()),
                    "P(complaint night | above t)": (_cn & _hi).sum() / max(_hi.sum(), 1),
                    "P(complaint night | not above t)": (_cn & ~_hi).sum() / max((~_hi).sum(), 1),
                    "P(above t | complaint night)": (_cn & _hi).sum() / max(_cn.sum(), 1),
                }
            )
    _tbl = pd.DataFrame(_rows).round(2)
    mo.ui.table(_tbl, selection=None, page_size=15)
    return


@app.cell
def _(mo):
    mo.md(r"""
    ## 7. Points for the session

    - **Complaints rise monotonically with H2S**, with no obvious step at 30 ppb:
      the complaint rate roughly doubles from the 10–30 bin to the 50–100 bin
      and keeps climbing. 30 ppb is a reasonable line but not a natural one.
    - **Most complaints arrive the morning after.** Any complaint-based
      verification of a nightly forecast must credit the *previous* night.
    - **Complaint location is the source location**, so complaints cannot be
      apportioned to stations. A nightly, valley-wide complaint count is the
      usable quantity.
    - **The complaint record is itself a candidate target**: "will tonight be
      a complaint night" is closer to what APCD is judged on than any ppb.
    """)
    return


if __name__ == "__main__":
    app.run()
