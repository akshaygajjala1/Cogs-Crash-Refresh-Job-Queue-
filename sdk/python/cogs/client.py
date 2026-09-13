"""Thin HTTP client over the Cogs wire protocol. See ../../PROTOCOL.md."""

from __future__ import annotations

import json
from typing import Any, Optional
from urllib import request as urlrequest
from urllib.error import HTTPError


class ApiError(Exception):
    def __init__(self, status: int, code: str, message: str):
        super().__init__(f"cogs: {status} {code}: {message}")
        self.status = status
        self.code = code
        self.message = message


class Client:
    """Raw HTTP client. Session reuse (keep-alive) matters at any real claim
    rate; urllib doesn't pool connections the way requests/httpx do, so a
    production deployment should swap this for one of those and keep a single
    Session/Client alive for the process lifetime rather than one per call.
    """

    def __init__(self, base_url: str, token: str = ""):
        self.base_url = base_url.rstrip("/")
        self.token = token

    def _request(self, method: str, path: str, body: Optional[dict] = None) -> dict:
        data = json.dumps(body).encode("utf-8") if body is not None else None
        req = urlrequest.Request(self.base_url + path, data=data, method=method)
        req.add_header("Content-Type", "application/json")
        if self.token:
            req.add_header("Authorization", f"Bearer {self.token}")
        try:
            with urlrequest.urlopen(req, timeout=60) as resp:
                raw = resp.read()
                return json.loads(raw) if raw else {}
        except HTTPError as e:
            raw = e.read()
            try:
                payload = json.loads(raw)
                err = payload.get("error", {})
                raise ApiError(e.code, err.get("code", "unknown"), err.get("message", str(e))) from None
            except json.JSONDecodeError:
                raise ApiError(e.code, "unknown", raw.decode("utf-8", "replace")) from None

    def enqueue(self, queue: str, payload: Any, idempotency_key: str = "") -> dict:
        return self._request(
            "POST",
            "/v1/jobs",
            {"queue": queue, "payload": payload, "idempotency_key": idempotency_key},
        )

    def claim(self, queues: list[str], batch: int, wait_ms: int, worker_id: str) -> dict:
        return self._request(
            "POST",
            "/v1/jobs/claim",
            {"queues": queues, "batch": batch, "wait_ms": wait_ms, "worker_id": worker_id},
        )

    def renew(self, queue: str, job_id: str) -> dict:
        """Raises ApiError(status=409) when the lease is already gone. Callers
        must treat that as "stop working", not as a retryable failure — see
        PROTOCOL.md, "Why a 409 on renew means stop, not retry"."""
        return self._request("POST", f"/v1/jobs/{job_id}/renew", {"queue": queue})

    def ack(self, queue: str, ids: list[str]) -> dict:
        return self._request("POST", "/v1/jobs/ack", {"queue": queue, "ids": ids})

    def fail(self, queue: str, job_id: str, error: str, attempt: int) -> dict:
        return self._request(
            "POST",
            f"/v1/jobs/{job_id}/fail",
            {"queue": queue, "error": error, "attempt": attempt},
        )
