from __future__ import annotations
import os
import time
import re
import requests
from typing import Optional, Tuple

def _headers():
    ua = os.getenv("SEC_USER_AGENT")
    if not ua:
        raise RuntimeError("Missing SEC_USER_AGENT env var.")
    return {"User-Agent": ua, "Accept-Encoding": "gzip, deflate"}

def _cik_digits(cik: str) -> str:
    return str(int(cik))  # remove leading zeros

def accession_to_nodash(acc: str) -> str:
    return acc.replace("-", "")

def fetch_filing_index(cik: str, accession: str, throttle_seconds: float = 0.25) -> str:
    cikd = _cik_digits(cik)
    acc_n = accession_to_nodash(accession)
    url = f"https://www.sec.gov/Archives/edgar/data/{cikd}/{acc_n}/index.json"
    time.sleep(throttle_seconds)
    r = requests.get(url, headers=_headers(), timeout=30)
    r.raise_for_status()
    return r.text

def pick_document_from_index(index_json_text: str) -> Optional[str]:
    # We keep this deliberately simple: find first .txt or .htm entry
    # index.json lists files; we regex for names.
    names = re.findall(r'"name"\s*:\s*"([^"]+)"', index_json_text)
    if not names:
        return None
    # Prefer .txt (often full submission), then .htm
    for ext in (".txt", ".htm", ".html"):
        for n in names:
            if n.lower().endswith(ext):
                return n
    return names[0]

def fetch_document_text(cik: str, accession: str, filename: str, throttle_seconds: float = 0.25) -> str:
    cikd = _cik_digits(cik)
    acc_n = accession_to_nodash(accession)
    url = f"https://www.sec.gov/Archives/edgar/data/{cikd}/{acc_n}/{filename}"
    time.sleep(throttle_seconds)
    r = requests.get(url, headers=_headers(), timeout=30)
    r.raise_for_status()
    return r.text

def html_to_text(s: str) -> str:
    # crude but ok for keyword scanning
    s = re.sub(r"(?is)<script.*?>.*?</script>", " ", s)
    s = re.sub(r"(?is)<style.*?>.*?</style>", " ", s)
    s = re.sub(r"(?is)<.*?>", " ", s)
    s = re.sub(r"\s+", " ", s)
    return s.strip()

def fetch_8k_text(cik: str, accession: str) -> Tuple[str, str]:
    idx = fetch_filing_index(cik, accession)
    filename = pick_document_from_index(idx) or ""
    raw = fetch_document_text(cik, accession, filename)
    return filename, html_to_text(raw)
