
## SD Beach Info (Current Status / Advisories / Closures)

**New site (as of ~2025):**
https://cosdapps.sandiegocounty.gov/sdbeachinfo/

The old site (www.sdbeachinfo.com) now redirects here. The new site is an OutSystems reactive web app.

### CoSD OutSystems API

The new site requires JavaScript-initialized session cookies (`nr1Users`, `nr2Users`, `osVisitor`, `osVisit`) and an `X-CSRFToken` header (extracted from the `crf=` field in the URL-decoded `nr2Users` cookie). Plain HTTP requests get 403. Use Playwright to load the page first.

All endpoints are POSTed to:
```
https://cosdapps.sandiegocounty.gov/sdbeachinfo/screenservices/CoSD_Beach_Water_CW/MainFlow/<Block>/<ActionName>
```

Request body structure:
```json
{
  "versionInfo": {"moduleVersion": "<token from /moduleservices/moduleversioninfo>", "apiVersion": "<captured at runtime>"},
  "viewName": "MainFlow.Home",
  "screenData": {"variables": {...}},
  "inputParameters": {"StartIndex": 0, "MaxRecords": 900}
}
```

**Get module version:**
GET `https://cosdapps.sandiegocounty.gov/sdbeachinfo/moduleservices/moduleversioninfo`
Returns `{"versionToken": "..."}` — pass as `moduleVersion` in all requests.

**Key endpoints (HomeBlockNew):**

`ScreenDataSetGetSiteById` — returns site list filtered by EventTypeId:
- `EventTypeId: 0` → all 89 monitoring sites with coordinates
- `EventTypeId: 1` → advisory sites only
- `EventTypeId: 2` → closure sites only

apiVersion: captured at runtime from intercepted browser requests (changes with deployments).
Screen variables: `ShowDetails, MapCenter, SiteIdAux, EventTypeId, ZoomCurrent, id, _idInDataFetchStatus`

**Key endpoints (BlockMarkup):**

`ScreenDataSetGetEventsList` — event history for a single site:
- Screen variables: `{"SiteId": "<id>", "_siteIdInDataFetchStatus": 1}`
- apiVersion: `JqlpdF6Psvnxe_pXo1Jjtw` (as of 2026-06)
- Returns events with `IssueDateTime`, `StatusId`, `DescriptionIssue`

`ScreenDataSetGetEventsData` — detailed event data for a site
- apiVersion: `CbxGxYvKg93xpMEAMzMgPw` (as of 2026-06)

`ScreenDataSetGetEvents` — event data (another variant)
- apiVersion: `X6kHzOPIWzBFjbOVvF8Oog` (as of 2026-06)

`ScreenDataSetGetEventsIsActive` — whether a site's event is currently active
- apiVersion: `_NIYQ5IUUP_BTHGXD24HHQ` (as of 2026-06)

### Site data returned by GetSiteById
Each site has: `Id`, `LocationName`, `BeachName`, `City`, `Region`, `Latitude`, `Longitude`, `Description`, `StatusId`

Note: `StatusId` in region list endpoints (`GetRegionsSouth`, `GetRegionsCentral`, `GetRegionsNort`) is always 0 — do NOT use those to determine status. Use `EventTypeId` filter on `GetSiteById` instead.

### Historical Closures / Advisory Archive

Historical closure info over time:
https://www.waterboards.ca.gov/water_issues/programs/beaches/search_beach_advisory.html

---

## CA BeachWatch (Sample Analysis Data)

POST to get results page (sets session cookies):
```
https://beachwatch.waterboards.ca.gov/public/result.php
Form: County=10&stationID=&parameter=&qualifier=&method=&created=&year=2024&sort=`SampleDate`&sortOrder=DESC&submit=Search
```

Then GET to download CSV (uses cookies from above):
```
https://beachwatch.waterboards.ca.gov/public/export.php
```

---

## South Region GI Illness Concerns

https://www.sandiegocounty.gov/content/sdc/hhsa/programs/phs/community_epidemiology/GI-Concerns.html

---

## California Safe to Swim Data

https://mywaterquality.ca.gov/safe-to-swim/content/interactive_map/index.html

Sites: https://data.ca.gov/dataset/surface-water-fecal-indicator-bacteria-results/resource/848d2e3f-2846-449c-90e0-9aaf5c45853e
Geometric means: https://data.ca.gov/dataset/surface-water-fecal-indicator-bacteria-results/resource/15a63495-8d9f-4a49-b43a-3092ef3106b9
https://data.ca.gov/dataset/surface-water-fecal-indicator-bacteria-results/resource/1987c159-ce07-47c6-8d4f-4483db6e6460

---

## Related Resources

EPA Beacon:
https://beacon.epa.gov/ords/beacon2/r/beacon_apex/beacon2/map-page
Standards: https://beacon.epa.gov/ords/beacon2/r/beacon_apex/beacon2/beach-profile-details?beach_id=CA068221&year=2024&debug=YES

San Diego Water Quality GitHub:
https://github.com/san-diego-water-quality/water-quality-project

NowCast model:
https://github.com/rtsearcy/NowCast
