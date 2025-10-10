Approval workflow for Netlify apps

We have apps hosted in netlify. We would like to have a preview and a public website.
These websites are next.js rendered applications. 
We will trigger a slack automation/workflow when
We want to trigger an automation when a dagster build step completes.

I think the steps will be:
* when a specific dagster asset completes, it needs so trigger the slack bolt automation.
* (step: preview deploy) this will send a message with that a preview is being deployed, with a link to the url (NETLIFY_PREVIEW_URL)  of the deploy preview, and options to 'Approve' or 'Reject'
* (step: approve deploy) if the user click 'approve'
  * the slack automation will do and HTTP POST to a url (NETILFY_PRODUCTION_HOOK)
  * the buttons are disabled or removed
  * a message saying approved is added to the channel, and a message with a link to the production deploy (NETLIFY_PRODUCTION_URL)
* (step: reject deploy) if the user clicks 'reject'
  * a message saying edit the prompts in airtable (message should be configurable, NETLIFY_REJECT_MESSAGE)
  * provide a link with the message 'trigger preview deploy when done editing'
  * a button with "trigger preview"
  * the slack automation will do and HTTP POST to a url (NETILFY_PREVIEW_HOOK)
  * button  "trigger preview" will be disabled or removed
  * This will restart the automation at: (step: preview deploy)

This automation should be generic, so that additional preview/production services can be passed to it using the NETLIFY_ parameters 

