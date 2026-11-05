import marimo

__generated_with = "0.24.2"
app = marimo.App(
    width="medium",
    app_title="Break points in the H2S observations",
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
    # Break points in the H2S observations

    **Question for the planning session.** 30 ppb is a health reference and 70 ppb
    the ATSDR acute MRL. Independently of those, does the *measured* record have
    natural breaks: levels at which the distribution changes regime, or at which
    a second and a third monitor start to register? Three views, NESTOR first,
    then the three monitors together.

    Data: the hourly station record and the nightly summary, production bucket.
    """)
    return


@app.cell
def _(d):
    hourly = d.hourly()
    nightly = d.nightly()
    nestor = hourly[hourly["site_name"] == "NESTOR - BES"]
    return hourly, nestor, nightly


@app.cell
def _(mo, nestor):
    _v = nestor["H2S"]
    mo.md(
        f"""
    ## 1. NESTOR hourly values

    {len(_v):,} measured hours from {nestor.time.min():%Y-%m-%d} to {nestor.time.max():%Y-%m-%d};
    resolution 0.1 ppb, {(_v < 1).mean():.0%} of hours below 1 ppb.
    Quantiles (ppb): median {_v.median():.1f}, 90th {_v.quantile(.9):.0f}, 95th {_v.quantile(.95):.0f},
    99th {_v.quantile(.99):.0f}, 99.9th {_v.quantile(.999):.0f}.

    ### 1a. Where the survival curve bends

    On log-log axes the share of hours above a level falls as a straight line while
    one process dominates; a change of slope is a change of regime.
    """
    )
    return


@app.cell
def _(alt, mo, nestor, np, pd):
    _v = nestor["H2S"].to_numpy()
    _levels = np.logspace(np.log10(1), np.log10(900), 80)
    _surv = pd.DataFrame({"ppb": _levels, "share_above": [(_v >= t).mean() for t in _levels]})
    _surv = _surv[_surv["share_above"] > 0]
    _rows = []
    for _lo, _hi in ((5, 15), (15, 40), (40, 100), (100, 300), (300, 900)):
        _xs = np.log10(np.linspace(_lo, _hi, 8))
        _ys = np.log10([max((_v >= t).mean(), 1e-9) for t in np.linspace(_lo, _hi, 8)])
        _rows.append({"range (ppb)": f"{_lo}–{_hi}", "hours in range": int(((_v >= _lo) & (_v < _hi)).sum()), "log-log slope": round(float(np.polyfit(_xs, _ys, 1)[0]), 2)})
    slopes = pd.DataFrame(_rows)
    _chart = (
        alt.Chart(_surv)
        .mark_line(color="#2a78d6", strokeWidth=2)
        .encode(
            x=alt.X("ppb:Q", scale=alt.Scale(type="log", domain=[1, 900]), title="H2S (ppb, log)"),
            y=alt.Y("share_above:Q", scale=alt.Scale(type="log"), title="share of hours at or above (log)"),
            tooltip=[alt.Tooltip("ppb:Q", format=".0f"), alt.Tooltip("share_above:Q", format=".4f")],
        )
        .properties(width=520, height=260, title="NESTOR hourly H2S: survival curve")
    )
    _rules = alt.Chart(pd.DataFrame({"ppb": [40, 100, 300]})).mark_rule(color="#eb6834", strokeDash=[4, 4]).encode(x="ppb:Q")
    mo.vstack([mo.ui.altair_chart(_chart + _rules), mo.ui.table(slopes, selection=None)])
    return


@app.cell
def _(mo):
    mo.md(r"""
    The slope is the same from 5 to 40 ppb, steepens at about 40, again at about
    100, and the tail runs out above 300. Hourly there is no break at 30 ppb.

    ### 1b. Density of hourly values above 1 ppb

    A kernel density on the log scale. Valleys are candidate breaks; the result
    depends on the smoothing, so three bandwidths are shown.
    """)
    return


@app.cell
def _(mo, nestor, np, pd):
    from scipy.stats import gaussian_kde

    def density_breaks(values, bandwidths, lo=1, hi=1000):
        x = np.log10(values[values > lo])
        grid = np.linspace(np.log10(lo), np.log10(hi), 600)
        rows, curves = [], []
        for bw in bandwidths:
            k = gaussian_kde(x, bw_method=bw)(grid)
            valleys = [round(10 ** grid[i]) for i in range(1, len(grid) - 1) if k[i] < k[i - 1] and k[i] < k[i + 1]]
            modes = [round(10 ** grid[i]) for i in range(1, len(grid) - 1) if k[i] > k[i - 1] and k[i] > k[i + 1]]
            rows.append({"bandwidth (log10)": bw, "valleys (ppb)": ", ".join(map(str, valleys)) or "none", "modes (ppb)": ", ".join(map(str, modes))})
            curves.append(pd.DataFrame({"ppb": 10 ** grid, "density": k, "bandwidth": str(bw)}))
        return pd.DataFrame(rows), pd.concat(curves, ignore_index=True)

    hourly_breaks, hourly_curves = density_breaks(nestor["H2S"], (0.08, 0.12, 0.18))
    mo.ui.table(hourly_breaks, selection=None)
    return density_breaks, hourly_curves


@app.cell
def _(alt, hourly_curves, mo):
    _chart = (
        alt.Chart(hourly_curves)
        .mark_line(strokeWidth=2)
        .encode(
            x=alt.X("ppb:Q", scale=alt.Scale(type="log", domain=[1, 1000]), title="H2S (ppb, log)"),
            y=alt.Y("density:Q", title="density on the log scale"),
            color=alt.Color("bandwidth:N", scale=alt.Scale(range=["#2a78d6", "#eb6834", "#1baf7a"]), legend=alt.Legend(title="bandwidth")),
        )
        .properties(width=520, height=240, title="NESTOR hourly values above 1 ppb")
    )
    mo.ui.altair_chart(_chart)
    return


@app.cell
def _(mo):
    mo.md(r"""
    ## 2. NESTOR nightly peaks

    The nightly peak distribution is multimodal and its valleys hold up across
    smoothing choices, which the hourly ones do not.
    """)
    return


@app.cell
def _(alt, density_breaks, mo, nightly, pd):
    _p = nightly.loc[nightly["site_name"] == "NESTOR - BES", "h2s_max"]
    nightly_breaks, _curves = density_breaks(_p, (0.10, 0.15, 0.20))
    _chart = (
        alt.Chart(_curves)
        .mark_line(strokeWidth=2)
        .encode(
            x=alt.X("ppb:Q", scale=alt.Scale(type="log", domain=[1, 1000]), title="night peak H2S (ppb, log)"),
            y=alt.Y("density:Q", title="density on the log scale"),
            color=alt.Color("bandwidth:N", scale=alt.Scale(range=["#2a78d6", "#eb6834", "#1baf7a"]), legend=alt.Legend(title="bandwidth")),
        )
        .properties(width=520, height=240, title=f"NESTOR nightly peaks ({len(_p)} nights)")
    )
    _rules = alt.Chart(pd.DataFrame({"ppb": [6, 14, 35, 100]})).mark_rule(color="#898781", strokeDash=[4, 4]).encode(x="ppb:Q")
    mo.vstack([mo.ui.table(nightly_breaks, selection=None), mo.ui.altair_chart(_chart + _rules)])
    return


@app.cell
def _(mo):
    mo.md(r"""
    Valleys near **6, 14, 35 and 100 ppb** with modes near 4, 10, 18, 48 and 150:
    five natural night classes. The 35 valley is the one nearest the County's
    30 ppb alert; the 100 valley is the one the threshold scan (notebook 04)
    found, with the 70 ppb tier sitting inside the elevated class.

    ## 3. When the other monitors register

    Hours and nights when all three monitors were measuring. First, how often an
    exceedance is seen by one, two or three monitors at once.
    """)
    return


@app.cell
def _(hourly, mo, nightly, pd):
    wide_h = hourly.pivot_table(index="time", columns="site_name", values="H2S", observed=True).dropna()
    wide_n = nightly.pivot_table(index="astro_day_date", columns="site_name", values="h2s_max", observed=True).dropna()
    _rows = []
    for _t in (5, 10, 20, 30, 50, 70, 100):
        _ch = (wide_h > _t).sum(axis=1)
        _cn = (wide_n > _t).sum(axis=1)
        _rows.append({"level (ppb)": _t, "hours: 1 monitor": int((_ch == 1).sum()), "2 monitors": int((_ch == 2).sum()), "3 monitors": int((_ch == 3).sum()),
                      "nights: 1 monitor": int((_cn == 1).sum()), "2 monitors ": int((_cn == 2).sum()), "3 monitors ": int((_cn == 3).sum())})
    mo.vstack([mo.md(f"{len(wide_h):,} hours and {len(wide_n)} nights with all three monitors measured."), mo.ui.table(pd.DataFrame(_rows), selection=None)])
    return wide_h, wide_n


@app.cell
def _(mo):
    mo.md(r"""
    ### 3a. The other two monitors as NESTOR's night peak rises
    """)
    return


@app.cell
def _(mo, pd, wide_n):
    _bins = [-1, 5, 10, 20, 30, 50, 70, 100, 200, 5000]
    _labels = ["≤5", "5–10", "10–20", "20–30", "30–50", "50–70", "70–100", "100–200", ">200"]
    _w = wide_n.copy()
    _w["nestor_bin"] = pd.cut(_w["NESTOR - BES"], _bins, labels=_labels)
    _g = _w.groupby("nestor_bin", observed=True)
    response_n = pd.DataFrame({
        "NESTOR night peak (ppb)": _labels,
        "nights": _g.size().reindex(_labels).fillna(0).astype(int).values,
        "San Ysidro > 5": _g["SAN YSIDRO"].apply(lambda s: (s > 5).mean()).reindex(_labels).values,
        "IB Civic > 5": _g["IB CIVIC CTR"].apply(lambda s: (s > 5).mean()).reindex(_labels).values,
        "all three > 5": _g.apply(lambda g: ((g["SAN YSIDRO"] > 5) & (g["IB CIVIC CTR"] > 5)).mean(), include_groups=False).reindex(_labels).values,
        "San Ysidro > 30": _g["SAN YSIDRO"].apply(lambda s: (s > 30).mean()).reindex(_labels).values,
        "IB Civic > 30": _g["IB CIVIC CTR"].apply(lambda s: (s > 30).mean()).reindex(_labels).values,
        "San Ysidro median": _g["SAN YSIDRO"].median().reindex(_labels).values,
        "IB Civic median": _g["IB CIVIC CTR"].median().reindex(_labels).values,
    })
    mo.ui.table(response_n.round(2), selection=None, page_size=10)
    return (response_n,)


@app.cell
def _(alt, mo, response_n):
    _long = response_n.melt(id_vars="NESTOR night peak (ppb)", value_vars=["San Ysidro > 5", "IB Civic > 5", "all three > 5", "San Ysidro > 30", "IB Civic > 30"], var_name="measure", value_name="share")
    _chart = (
        alt.Chart(_long)
        .mark_line(point=alt.OverlayMarkDef(size=45), strokeWidth=2)
        .encode(
            x=alt.X("NESTOR night peak (ppb):N", sort=list(response_n["NESTOR night peak (ppb)"])),
            y=alt.Y("share:Q", axis=alt.Axis(format=".0%"), title="share of nights"),
            color=alt.Color("measure:N", scale=alt.Scale(range=["#2a78d6", "#1baf7a", "#4a3aa7", "#86b6ef", "#22c48a"]), legend=alt.Legend(title=None, orient="bottom", columns=3)),
            tooltip=["measure", "NESTOR night peak (ppb)", alt.Tooltip("share:Q", format=".0%")],
        )
        .properties(width=560, height=260, title="How the other monitors respond as NESTOR's night rises")
    )
    mo.ui.altair_chart(_chart)
    return


@app.cell
def _(mo):
    mo.md(r"""
    ### 3b. The same, hour by hour
    """)
    return


@app.cell
def _(mo, pd, wide_h):
    _bins = [-1, 1, 2, 5, 10, 20, 30, 50, 100, 200, 5000]
    _labels = ["≤1", "1–2", "2–5", "5–10", "10–20", "20–30", "30–50", "50–100", "100–200", ">200"]
    _w = wide_h.copy()
    _w["nestor_bin"] = pd.cut(_w["NESTOR - BES"], _bins, labels=_labels)
    _g = _w.groupby("nestor_bin", observed=True)
    response_h = pd.DataFrame({
        "NESTOR hour (ppb)": _labels,
        "hours": _g.size().reindex(_labels).fillna(0).astype(int).values,
        "San Ysidro > 5": _g["SAN YSIDRO"].apply(lambda s: (s > 5).mean()).reindex(_labels).values,
        "IB Civic > 5": _g["IB CIVIC CTR"].apply(lambda s: (s > 5).mean()).reindex(_labels).values,
        "both > 5": _g.apply(lambda g: ((g["SAN YSIDRO"] > 5) & (g["IB CIVIC CTR"] > 5)).mean(), include_groups=False).reindex(_labels).values,
        "San Ysidro > 30": _g["SAN YSIDRO"].apply(lambda s: (s > 30).mean()).reindex(_labels).values,
        "IB Civic > 30": _g["IB CIVIC CTR"].apply(lambda s: (s > 30).mean()).reindex(_labels).values,
    })
    mo.ui.table(response_h.round(2), selection=None, page_size=10)
    return


@app.cell
def _(mo, wide_h):
    _rows = []
    for _s in ("SAN YSIDRO", "IB CIVIC CTR"):
        _m = wide_h[_s] > 30
        _rows.append(f"- When **{_s}** is above 30 ppb ({int(_m.sum())} hours): NESTOR median {wide_h.loc[_m, 'NESTOR - BES'].median():.0f} ppb, above 30 in {(wide_h.loc[_m, 'NESTOR - BES'] > 30).mean():.0%} of those hours, above 5 in {(wide_h.loc[_m, 'NESTOR - BES'] > 5).mean():.0%}.")
    mo.md("### 3c. Direction\n\n" + "\n".join(_rows) + "\n\nIB Civic Center exceeding is a valley-wide event with NESTOR in it; San Ysidro exceeding often is not, so San Ysidro has a source regime of its own.")
    return


@app.cell
def _(mo):
    mo.md(r"""
    ## 4. Reading

    - **Hourly, NESTOR has no break at 30 ppb.** The first regime change is near
      40, the second near 100, and the tail ends above 300.
    - **Nightly, the peaks fall into five classes** with valleys near 6, 14, 35 and
      100 ppb. 35 is the observation-based neighbour of the 30 ppb alert.
    - **The second monitor wakes at about 5 ppb at NESTOR**, registers on most
      nights by 20–30, and all three read above 5 on about half the nights once
      NESTOR passes 30. The other monitors only start exceeding 30 themselves
      once NESTOR is above 100, and do so on most nights above 200.
    - **A valley-wide flag** defined on IB Civic Center is well supported (an IB
      exceedance means a NESTOR exceedance 9 times in 10); one that requires
      San Ysidro is not.

    Candidate observation-based tiers at NESTOR: 5, 35–40, 100, 200.
    """)
    return


if __name__ == "__main__":
    app.run()
