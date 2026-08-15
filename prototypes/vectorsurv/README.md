# VectorSurv prototypes — California / San Diego

Standalone prototype pages for the VectorSurv (maps.vectorsurv.org) vector-borne
surveillance data that the `pathogens` code location now ingests weekly
(`workflows/pathogens/src/pathogens/assets/vectorsurv.py`).

| Page | Layers | Detail on click |
|------|--------|-----------------|
| `dengue_invasive.html` | Invasive *Aedes* surveillance regions and dengue transmission-risk regions (subcounty polygons) | Species detection summary + weekly mosquito counts chart (2010–present); weekly dengue risk-index chart |
| `arbovirus.html` | Arbovirus activity (WNV, SLEV, WEEV, …): positive mosquito pools, dead birds, sentinel seroconversions by city | Positives-by-year stacked chart and top-cities table |

Both default to a **San Diego** view with an **All California** toggle.

## Running

The pages are self-contained (Leaflet from CDN) and read the public VectorSurv
map API (`https://mathew.vectorsurv.org/v2`, CORS-enabled) directly, decoding
the encoded-polyline geometries client-side. Serve the directory and open a
page:

```bash
cd prototypes/vectorsurv
python3 -m http.server 8080
# http://localhost:8080/dengue_invasive.html
# http://localhost:8080/arbovirus.html
```

`?api=<base-url>` overrides the API base, e.g. to point at a mirror of the raw
payloads stored by the Dagster assets under
`pathogens/vectorborne/raw/vectorsurv/` in S3.

## Data notes

* Weekly invasive counts: weeks without surveillance are omitted by the API;
  `0` means surveyed with none collected.
* The dengue risk index is a weekly modeled transmission-risk value (0 = no
  risk).
* The mosquito-abundance (AMoR) endpoints of the same API require a VectorSurv
  Gateway login and are not used here.
