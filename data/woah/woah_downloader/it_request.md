# IT Request: Azure AD App Registration for Automated SharePoint Access

**To:** UCSD IT Services / ITS Identity & Access Management  
**From:** David Valentine, dwvalentine@ucsd.edu  
**Subject:** Request for Azure AD App Registration — Automated SharePoint Data Pipeline

---

## Summary

I am requesting an **Azure Active Directory app registration** with application-level
permissions to access a specific SharePoint site. This will allow a scheduled data
pipeline to download files from SharePoint without requiring interactive login or
multi-factor authentication on each run.

---

## Business Justification

I maintain an automated epidemiological data pipeline that processes weekly disease
surveillance data published by the OIE (World Organisation for Animal Health) to the
UCSD-affiliated SharePoint site below. The pipeline runs on a weekly schedule in
Dagster (our workflow orchestrator) and must operate unattended overnight.

Currently, interactive login with Duo MFA is required each session, which blocks
fully automated operation. An app registration with a client secret or certificate
would allow the service to authenticate without user interaction.

---

## Technical Details

**SharePoint site to access:**
```
https://oieoffice365.sharepoint.com/sites/PeriodicaldataextractionsOIE-WAHIS
```

**Files accessed** (read-only):
- `Metadata_WeeklyExtraction.xlsx` — metadata index file
- `infur_{date}.xlsx` — weekly data extract (latest file only)

**What I need from IT:**

1. **An app registration** in Azure AD (Entra ID) with:
   - Application (not delegated) permissions: `Sites.Selected` on SharePoint
     *(narrowly scoped to the one site above — not all of SharePoint)*
   - A **client secret** (or certificate) I can store securely in our pipeline

2. The following values after registration:
   - `Application (client) ID`
   - `Directory (tenant) ID`
   - `Client secret value` (or path to certificate)

**Why `Sites.Selected` (not `Sites.Read.All`):**  
`Sites.Selected` is the principle-of-least-privilege permission — it grants access
only to explicitly nominated sites, not the entire SharePoint tenant. This is
Microsoft's recommended approach for service accounts.

---

## Security Commitments

- The client secret will be stored as an **environment variable** on our secured
  compute instance, not in source code or version control.
- Access is **read-only** — no write, delete, or modify permissions requested.
- The pipeline runs on a UCSD-managed system.
- I will rotate the client secret annually or upon any suspected compromise.

---

## Reference

Microsoft documentation for this pattern:  
https://learn.microsoft.com/en-us/sharepoint/dev/solution-guidance/security-apponly-azuread

If it is easier, I am happy to schedule a call to walk through the setup together.

**Contact:** dwvalentine@ucsd.edu

---
*Generated for UCSD ITS submission — edit as needed before sending.*
