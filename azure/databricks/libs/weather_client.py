"""Parallel Weatherstack client.

Mirrors v1's src/extract.py shape:
  * ThreadPoolExecutor with a fixed number of workers.
  * One HTTP request per city, no batching (the Weatherstack free tier
    has a per-request quota, not a per-call batching API).
  * Surfaces non-200 responses as exceptions so the notebook can decide
    whether to fail the whole run or carry on and log.

The function is intentionally pure: it takes the api_key, city list, and
a max_workers count, and returns a list of dicts. No Spark, no dbutils,
no filesystem — that lets tests/test_silver_schema.py mock the HTTP layer
with a stub.

Weatherstack response shape we depend on (v1 already proved this works):
    {
      "success": true,
      "location": {"name": "...", "country": "...", "region": "...",
                   "localtime": "2026-07-19 14:00"},
      "current": {"temperature": 31, "humidity": 42, "wind_speed": 11,
                  "weather_descriptions": ["Sunny"], ...}
    }

On error: {"success": false, "error": {"code": ..., "info": ...}}.
We treat !success as a hard failure for that city and re-raise; the
notebook decides the cluster-wide policy.
"""
from __future__ import annotations

import json
import time
import urllib.error
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Iterable, List, Mapping, Sequence


WEATHERSTACK_BASE_URL = "http://api.weatherstack.com/current"


class WeatherstackError(RuntimeError):
    """Raised when the API returns a non-success payload or an HTTP error."""


def _fetch_one(api_key: str, city: Mapping[str, str], timeout: float = 15.0) -> dict:
    """Call Weatherstack for a single city. Returns the parsed JSON body.

    Raises WeatherstackError on transport errors, HTTP non-200, or a
    `success: false` payload.
    """
    query = urllib.parse.urlencode({
        "access_key": api_key,
        "query": city["name"],
        "units": "m",
    })
    url = f"{WEATHERSTACK_BASE_URL}?{query}"
    try:
        with urllib.request.urlopen(url, timeout=timeout) as resp:
            body = resp.read().decode("utf-8")
    except urllib.error.URLError as exc:
        raise WeatherstackError(f"transport error for {city['name']}: {exc}") from exc
    try:
        payload = json.loads(body)
    except json.JSONDecodeError as exc:
        raise WeatherstackError(f"non-JSON response for {city['name']}: {body[:200]}") from exc
    if not payload.get("success", True):
        err = payload.get("error", {})
        raise WeatherstackError(
            f"Weatherstack returned success=false for {city['name']}: "
            f"code={err.get('code')} info={err.get('info')}"
        )
    return payload


def fetch_all(api_key: str, cities: Sequence[Mapping[str, str]],
              max_workers: int = 10) -> List[dict]:
    """Fetch Weatherstack for every city in parallel.

    Returns the list of payloads in the SAME order as `cities` (we sort
    by future completion, not by the order futures complete). The list
    is parallel to `cities`, so caller can zip them.
    """
    if not cities:
        return []
    results: List[dict | None] = [None] * len(cities)
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        future_to_index = {
            pool.submit(_fetch_one, api_key, city): i
            for i, city in enumerate(cities)
        }
        for fut in as_completed(future_to_index):
            i = future_to_index[fut]
            results[i] = fut.result()
    return [r for r in results if r is not None]  # type: ignore[misc]


def fetch_all_resilient(api_key: str, cities: Sequence[Mapping[str, str]],
                        max_workers: int = 10) -> tuple[List[dict], List[tuple[int, str]]]:
    """Same as fetch_all but never raises — returns (successes, failures).

    failures is a list of (city_index, error_message) tuples so the caller
    can decide whether to log-and-continue or fail the pipeline. This is
    what the Bronze notebook uses: a single bad city should not blank
    the whole hour.
    """
    if not cities:
        return [], []
    successes: dict[int, dict] = {}
    failures: List[tuple[int, str]] = []
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        future_to_index = {
            pool.submit(_fetch_one, api_key, city): i
            for i, city in enumerate(cities)
        }
        for fut in as_completed(future_to_index):
            i = future_to_index[fut]
            try:
                successes[i] = fut.result()
            except WeatherstackError as exc:
                failures.append((i, str(exc)))
    ordered = [successes[i] for i in sorted(successes)]
    return ordered, failures
