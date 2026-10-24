import marimo

__generated_with = "0.24.2"
app = marimo.App(width="medium", app_title="Choosing the upper threshold")


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
    # Is 100 ppb the right upper threshold?

    **Decision for session 1.** The County alerts on 30 ppb, and the 5 and 10 ppb
    classifiers stay as they are. The open question is the *upper* tier: 100 ppb
    was chosen by convention. Three things can tell us whether another level
    would serve better:

    1. **Sample size** — positive nights per station and per year.
    2. **Predictability** — a walk-forward nightly classifier at each candidate
       threshold, scored by average precision, recall at a fixed precision, and
       against persistence. Uses only features known the evening before (plus
       the night's own weather, so it is an upper bound).
    3. **Natural breaks** — in the distribution of nightly peaks, and in the
       complaint response.

    Reference points outside the data: the California 1-hour standard is 30 ppb;
    the ATSDR acute minimal risk level is 70 ppb; the WHO 24-hour guideline of
    150 µg/m³ is about 107 ppb, which is probably where "100" came from.
    """)
    return


@app.cell
def _(d, np):
    hourly = d.hourly()
    nightly = d.nightly().sort_values(["site_name", "night_start"]).reset_index(drop=True)
    _g = nightly.groupby("site_name", observed=True)
    nightly["prev_peak_log"] = np.log1p(nightly["prev_h2s_max"])
    nightly["prev_h30"] = _g["hours_above_30"].shift(1)
    nightly["prev_h5"] = _g["hours_above_5"].shift(1)
    nightly["prev3_peak_log"] = np.log1p(_g["h2s_max"].shift(1).rolling(3, min_periods=1).mean().reset_index(level=0, drop=True))
    nightly["prev7_peak_log"] = np.log1p(_g["h2s_max"].shift(1).rolling(7, min_periods=1).mean().reset_index(level=0, drop=True))
    nightly["m_sin"] = np.sin(2 * np.pi * nightly["month"] / 12)
    nightly["m_cos"] = np.cos(2 * np.pi * nightly["month"] / 12)
    nightly["wd_sin"] = np.sin(np.radians(nightly["wind_resultant_direction"]))
    nightly["wd_cos"] = np.cos(np.radians(nightly["wind_resultant_direction"]))
    THRESHOLDS = [30, 40, 50, 60, 75, 100, 150, 200]
    FEATURES = [
        "prev_peak_log", "prev_h30", "prev_h5", "prev3_peak_log", "prev7_peak_log",
        "m_sin", "m_cos", "night_length_hours", "sbiwtp_flow_mgd_mean",
        "temperature_2m_mean", "wind_speed_mean", "wind_resultant_speed", "wd_sin", "wd_cos", "wind_steadiness",
        "relative_humidity_2m_mean", "surface_pressure_mean", "tide_height_mean", "tide_height_max", "stable_atm_hours",
    ]
    return FEATURES, THRESHOLDS, hourly, nightly


@app.cell
def _(mo):
    mo.md(r"""
    ## 1. Sample size
    """)
    return


@app.cell
def _(THRESHOLDS, mo, nightly, pd):
    _rows = []
    for _site, _g in nightly.groupby("site_name", observed=True):
        for _t in THRESHOLDS:
            _pos = _g["h2s_max"] > _t
            _r = {"station": _site, "threshold": _t, "positive nights": int(_pos.sum()), "rate": round(_pos.mean(), 3)}
            for _y, _v in _pos.groupby(_g["astro_year"]).sum().items():
                _r[str(_y)] = int(_v)
            _rows.append(_r)
    sample = pd.DataFrame(_rows)
    mo.ui.table(sample, selection=None, page_size=24)
    return (sample,)


@app.cell
def _(alt, d, mo, sample):
    _chart = (
        alt.Chart(sample)
        .mark_line(point=alt.OverlayMarkDef(size=50), strokeWidth=2)
        .encode(
            x=alt.X("threshold:O", title="threshold (ppb)"),
            y=alt.Y("positive nights:Q", scale=alt.Scale(type="symlog", constant=5)),
            color=alt.Color("station:N", sort=d.STATIONS, scale=alt.Scale(domain=d.STATIONS, range=[d.STATION_COLORS[s] for s in d.STATIONS])),
            tooltip=["station", "threshold", "positive nights", "rate"],
        )
        .properties(width=520, height=240, title="Positive nights by threshold (symlog y)")
    )
    mo.ui.altair_chart(_chart)
    return


@app.cell
def _(mo):
    mo.md(r"""
    ## 2. Predictability by threshold

    For each candidate threshold, a RandomForest is trained on nightly rows and
    evaluated walk-forward (expanding window, four test blocks over the last 60%
    of the record, two seeds). Features are last night's peak and recent
    history, SBIWTP effluent, season, and the night's mean weather and tide.
    The weather is the *observed* night mean, so this is an upper bound on
    operational skill; the comparison *between* thresholds is what matters.

    - **AP**: average precision (area under the precision–recall curve); its
      no-skill value is the base rate.
    - **recall @ precision 0.5**: how many positive nights are caught if one
      alert in two may be a false alarm.
    - **persistence**: "same as last night", precision equals recall by
      construction.
    - Bootstrap 90% intervals come from resampling test nights.
    """)
    return


@app.cell
def _(FEATURES, np, pd):
    from sklearn.ensemble import RandomForestClassifier
    from sklearn.metrics import average_precision_score, precision_recall_curve, roc_auc_score

    def walk_forward_probs(df, threshold, seeds=(0, 1), n_folds=4, start_frac=0.4):
        df = df.dropna(subset=FEATURES + ["h2s_max"]).reset_index(drop=True)
        y = (df["h2s_max"] > threshold).astype(int).to_numpy()
        X = df[FEATURES].to_numpy()
        start = int(len(df) * start_frac)
        block = (len(df) - start) // n_folds
        prob = np.full(len(df), np.nan)
        for i in range(n_folds):
            s = start + i * block
            e = s + block if i < n_folds - 1 else len(df)
            if y[:s].sum() < 5:
                continue
            p = np.zeros(e - s)
            for seed in seeds:
                rf = RandomForestClassifier(n_estimators=300, min_samples_leaf=5, max_features="sqrt", class_weight="balanced", n_jobs=-1, random_state=seed)
                rf.fit(X[:s], y[:s])
                p += rf.predict_proba(X[s:e])[:, 1] / len(seeds)
            prob[s:e] = p
        m = ~np.isnan(prob)
        return y[m], prob[m], (df.loc[m, "prev_h2s_max"] > threshold).to_numpy()

    def recall_at_precision(y, p, floor):
        pr, rc, _ = precision_recall_curve(y, p)
        ok = pr[:-1] >= floor
        return rc[:-1][ok].max() if ok.any() else 0.0

    def score(y, p, pers):
        tp = (pers & (y == 1)).sum()
        return {
            "AUC": roc_auc_score(y, p),
            "AP": average_precision_score(y, p),
            "base rate": y.mean(),
            "recall @ precision 0.5": recall_at_precision(y, p, 0.5),
            "recall @ precision 0.7": recall_at_precision(y, p, 0.7),
            "persistence precision = recall": tp / max(y.sum(), 1),
        }

    def bootstrap(y, p, pers, n=300, seed=0):
        rng = np.random.default_rng(seed)
        aps, r50 = [], []
        for _ in range(n):
            idx = rng.integers(0, len(y), len(y))
            if y[idx].sum() < 2:
                continue
            aps.append(average_precision_score(y[idx], p[idx]))
            r50.append(recall_at_precision(y[idx], p[idx], 0.5))
        return np.percentile(aps, [5, 95]), np.percentile(r50, [5, 95])

    def scan(df, thresholds):
        rows = []
        for t in thresholds:
            y, p, pers = walk_forward_probs(df, t)
            if y.sum() < 3:
                continue
            r = {"threshold": t, "test nights": len(y), "test positives": int(y.sum()), **score(y, p, pers)}
            (ap_lo, ap_hi), (r_lo, r_hi) = bootstrap(y, p, pers)
            r["AP 90% CI"] = f"{ap_lo:.2f}–{ap_hi:.2f}"
            r["recall@0.5 90% CI"] = f"{r_lo:.2f}–{r_hi:.2f}"
            rows.append(r)
        return pd.DataFrame(rows)

    return (scan,)


@app.cell
def _(THRESHOLDS, nightly, scan):
    skill_nestor = scan(nightly[nightly["site_name"] == "NESTOR - BES"], THRESHOLDS)
    return (skill_nestor,)


@app.cell
def _(mo, skill_nestor):
    mo.vstack([mo.md("**NESTOR - BES**, the only station with a usable sample above 50 ppb:"), mo.ui.table(skill_nestor.round(3), selection=None, page_size=10)])
    return


@app.cell
def _(alt, mo, skill_nestor):
    _long = skill_nestor.melt(id_vars="threshold", value_vars=["AP", "recall @ precision 0.5", "persistence precision = recall", "base rate"], var_name="metric", value_name="value")
    _chart = (
        alt.Chart(_long)
        .mark_line(point=alt.OverlayMarkDef(size=50), strokeWidth=2)
        .encode(
            x=alt.X("threshold:O", title="threshold (ppb)"),
            y=alt.Y("value:Q", title=None, scale=alt.Scale(domain=[0, 1])),
            color=alt.Color("metric:N", scale=alt.Scale(domain=["AP", "recall @ precision 0.5", "persistence precision = recall", "base rate"], range=["#2a78d6", "#eb6834", "#1baf7a", "#898781"]), legend=alt.Legend(title=None, orient="bottom", columns=2)),
            tooltip=["metric", "threshold", alt.Tooltip("value:Q", format=".2f")],
        )
        .properties(width=520, height=260, title="NESTOR nightly-peak skill by threshold (walk-forward)")
    )
    mo.ui.altair_chart(_chart)
    return


@app.cell
def _(mo):
    mo.md(r"""
    ## 3. Natural breaks

    ### 3a. In the distribution of nightly peaks

    A kernel density of log-peak at NESTOR. Valleys are candidate breaks: a
    threshold placed in a valley separates populations rather than cutting
    through one.
    """)
    return


@app.cell
def _(alt, mo, nightly, np, pd):
    from scipy.stats import gaussian_kde

    _x = np.log10(nightly.loc[(nightly["site_name"] == "NESTOR - BES") & (nightly["h2s_max"] > 5), "h2s_max"])
    _grid = np.linspace(np.log10(5), np.log10(1000), 400)
    _k = gaussian_kde(_x, bw_method=0.15)(_grid)
    valleys = [10 ** _grid[i] for i in range(1, len(_grid) - 1) if _k[i] < _k[i - 1] and _k[i] < _k[i + 1]]
    peaks_ = [10 ** _grid[i] for i in range(1, len(_grid) - 1) if _k[i] > _k[i - 1] and _k[i] > _k[i + 1]]
    _df = pd.DataFrame({"ppb": 10 ** _grid, "density": _k})
    _line = alt.Chart(_df).mark_area(color="#2a78d6", opacity=0.25, line={"color": "#2a78d6", "strokeWidth": 2}).encode(
        x=alt.X("ppb:Q", scale=alt.Scale(type="log", domain=[5, 1000]), title="night peak H2S (ppb, log)"),
        y=alt.Y("density:Q", title="density (log scale)"),
    )
    _rules = alt.Chart(pd.DataFrame({"ppb": valleys})).mark_rule(color="#eb6834", strokeDash=[4, 4]).encode(x="ppb:Q")
    mo.vstack(
        [
            mo.md(f"Density valleys at **{', '.join(f'{v:.0f}' for v in valleys)} ppb**; modes at {', '.join(f'{v:.0f}' for v in peaks_)} ppb (nights with peak above 5 ppb, bandwidth 0.15 in log10)."),
            mo.ui.altair_chart((_line + _rules).properties(width=520, height=220, title="NESTOR nightly peaks: density with valleys marked")),
        ]
    )
    return


@app.cell
def _(mo):
    mo.md(r"""
    ### 3b. In the complaint response

    Odour complaints per 100 measured NESTOR hours by a finer H2S bin, and the
    share of NESTOR nights in each peak bin that drew 5 and 10 or more
    complaints.
    """)
    return


@app.cell
def _(d, hourly, mo, nightly, pd):
    _c = d.complaints()
    _od = _c[_c["is_odor"] & _c["in_trv"]]
    _nes = hourly[hourly["site_name"] == "NESTOR - BES"].set_index("time").sort_index()
    _nes = _nes[~_nes.index.duplicated()]
    _j = _od.merge(_nes[["H2S"]], left_on="hour_floor", right_index=True, how="inner")
    _bins = [-0.01, 5, 10, 20, 30, 40, 50, 75, 100, 150, 200, 10_000]
    _labels = ["≤5", "5–10", "10–20", "20–30", "30–40", "40–50", "50–75", "75–100", "100–150", "150–200", ">200"]
    _hours = pd.cut(_nes["H2S"], _bins, labels=_labels).value_counts().sort_index()
    _cm = pd.cut(_j["H2S"], _bins, labels=_labels).value_counts().sort_index()
    _nn = nightly[nightly["site_name"] == "NESTOR - BES"].copy()
    _nn["bin"] = pd.cut(_nn["h2s_max"], _bins, labels=_labels)
    _nights = _nn.groupby("bin", observed=True).agg(nights=("h2s_max", "size"), mean_complaints=("complaints_total", "mean"), ge5=("complaints_total", lambda s: (s >= 5).mean()), ge10=("complaints_total", lambda s: (s >= 10).mean()))
    response = pd.DataFrame({"H2S bin (ppb)": _labels, "measured hours": _hours.values, "complaints in those hours": _cm.values})
    response["complaints per 100 h"] = (100 * response["complaints in those hours"] / response["measured hours"]).round(0)
    response["nights with peak in bin"] = _nights["nights"].values
    response["mean complaints per night"] = _nights["mean_complaints"].round(1).values
    response["share of nights ≥5 complaints"] = _nights["ge5"].round(2).values
    response["share of nights ≥10 complaints"] = _nights["ge10"].round(2).values
    mo.ui.table(response, selection=None, page_size=12)
    return (response,)


@app.cell
def _(alt, mo, response):
    _long = response.melt(id_vars="H2S bin (ppb)", value_vars=["share of nights ≥5 complaints", "share of nights ≥10 complaints"], var_name="metric", value_name="share")
    _chart = (
        alt.Chart(_long)
        .mark_line(point=alt.OverlayMarkDef(size=50), strokeWidth=2)
        .encode(
            x=alt.X("H2S bin (ppb):N", sort=list(response["H2S bin (ppb)"]), title="NESTOR night peak (ppb)"),
            y=alt.Y("share:Q", axis=alt.Axis(format=".0%"), title="share of nights"),
            color=alt.Color("metric:N", scale=alt.Scale(range=["#2a78d6", "#eb6834"]), legend=alt.Legend(title=None, orient="bottom")),
            tooltip=["metric", "H2S bin (ppb)", alt.Tooltip("share:Q", format=".0%")],
        )
        .properties(width=520, height=240, title="How often a night at each level becomes a complaint night")
    )
    mo.ui.altair_chart(_chart)
    return


@app.cell
def _(mo):
    mo.md(r"""
    ## 4. Reading

    - **100 ppb is not random.** It sits in a density valley of the NESTOR
      nightly-peak distribution (modes near 50 and 150), and it approximates the
      WHO 24-hour guideline. But it is where skill falls: recall at 50% precision
      drops from about 0.87 at 75 ppb to about 0.69 at 100, and persistence
      precision from 0.57 to 0.54, because positives thin out.
    - **The complaint response steps at 50 ppb, not 100.** Per-hour complaint
      rate roughly doubles from the 30–40 bin to 50–75 and then plateaus; nights
      with ten or more complaints go from about one in ten below 50 ppb to about
      three in ten at 50–100, then to six in ten above 150.
    - **50 ppb keeps nearly the skill of 30 ppb** (AP 0.75 against 0.81; recall
      at 50% precision 0.90 against 0.91) with 174 positive NESTOR nights, and it
      is the first level that still has twenty-odd positive nights at the other
      two stations.
    - **Above 100 ppb is a NESTOR-only descriptor.** 150 ppb is where nights
      become reliably complaint-heavy, but there are 74 of them and skill is
      modest (AP 0.56).

    Candidate tiering for session 1: 30 ppb as the County alert, **50 ppb as the
    "elevated" tier at all stations**, and a NESTOR-only "severe" tier at 100 or
    150 ppb read off a single ordinal or quantile model rather than trained as a
    separate classifier. Treat the 75-vs-100 and 100-vs-150 differences as
    indicative only; the bootstrap intervals overlap.
    """)
    return


if __name__ == "__main__":
    app.run()
