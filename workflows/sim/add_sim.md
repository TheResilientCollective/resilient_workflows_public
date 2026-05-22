# project
We want to add a simulation model to this project.
I want a workflow similar to the workflows/sim/DOCUMENTATION.md

This will be for mpox.

when the data for the asset AssetKey(["mpox", "mpox_aggregated"]) is updated, it should trigger a run of the simulation model.
 The output of the simulation model should be stored in the s3 bucket specified in the config, and validated and transformed into a schema that can be stored  and S3.
 we do not want airtable for this, but we do want to trigger a webhook to build a website, and send messages to slack and email.


This is an config from the sims api.

```
"config": 49,
  "output_template": {
    "type": "cloud_storage",
    "provider": "s3",
    "bucket": "resilientmpox",
    "key_prefix": "api_run/{timestamp}_run{run_id}"
  },
  "cloud_credentials": {
    "s3": {
      "access_key_id": "resilientdata",
      "secret_access_key": "resilientdatasecret",
      "endpoint_url": "https://oss.resilientservice.mooo.com"
    }
  }
}

```

We want to have a sensor that when mpox data is updated, it triggers a run of the simulation model. The output of the simulation model should be stored in the s3 bucket specified in the config.
the

When the simulation model returns, it executes webhooks.
* copies most recent run to the latest path
* copies run to github repo for version control and record keeping.
* Optional LLM
* triggers a webhook to build a website.
Sends messages,
 * slack
 * email to an email list


