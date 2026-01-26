import os
import json
import requests

def notify(text: str) -> None:
    url = os.getenv("SLACK_WEBHOOK_URL")
    if not url:
        return

    requests.post(
        url,
        data=json.dumps({"text": text}),
        headers={"Content-Type": "application/json"},
        timeout=10,
    )