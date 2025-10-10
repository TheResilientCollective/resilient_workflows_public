
### Update Summary
apikey: 
baseId: appPLSM4Fub5wdspQ
summary record: recowLGi7NgFdWX7B
summary table: Widgets
import requests

tableId = "Widgets"
baseId = "appPLSM4Fub5wdspQ"
apiKey = ""
recordId = "recowLGi7NgFdWX7B"
url = f"https://api.airtable.com/v0/{baseId}/{tableId}/{recordId}"
text = "my updated text goes here"

headers = {
    "Authorization": f"Bearer {apiKey}",
    "Content-Type": "application/json"
}

body = {
    "fields": {
        "Text": text
    }
}

response = requests.patch(url, headers=headers, json=body)

print(response.status_code)
### Update updates
apikey: 
baseId: appPLSM4Fub5wdspQ
summary table: Updates
RsvPortalRecordId: rec4NITTQNAONirhd

import requests
tableId = "Updates"
baseId = "appPLSM4Fub5wdspQ"
apiKey = ""
RsvPortalRecordId = "rec4NITTQNAONirhd"
text = "My updated text here"

url = f"https://api.airtable.com/v0/{baseId}/{tableId}"
headers = {
    "Authorization": f"Bearer {apiKey}",
    "Content-Type": "application/json"
}
data = {
    "fields": {
        "Portal": [RsvPortalRecordId],
        "Update": text,
        "Status": "Published",
        # "Portal slug": portalSlug
    }
}

response = requests.post(url, headers=headers, json=data)

print(response.json())
