"""Placeholder settings so ``sim`` (and its Definitions) import without a real environment."""

import os

_PLACEHOLDERS = {
    "S3_BUCKET": "test-bucket",
    "S3_ADDRESS": "localhost",
    "S3_PORT": "9000",
    "S3_ACCESS_KEY": "k",
    "S3_SECRET_KEY": "s",
    "AIRTABLE_ACCESS_TOKEN": "t",
    "AIRTABLE_BASE_ID": "b",
    "OPENAI_API_KEY": "k",
    "OPENAI_BASE_URL": "http://localhost",
    "SLACK_TOKEN": "t",
    "RESILIENTSIMS_USERNAME": "u",
    "RESILIENTSIMS_PASSWORD": "p",
    "RESILIENTSIMS_SIMULATOR_ID": "1",
    "RESILIENTLLM_API_TOKEN": "test-token",
    "RESILIENTLLM_WEBHOOK": "https://agents.example.org/webhook/",
    "RESILIENTLLM_WEBHOOK_UUID": "report-uuid",
    "EMAIL_ENABLED": "false",
}
for key, value in _PLACEHOLDERS.items():
    os.environ.setdefault(key, value)
