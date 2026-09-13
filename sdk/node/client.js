// Thin HTTP client over the Cogs wire protocol. See ../../PROTOCOL.md.
//
// Uses Node's built-in fetch (18+) rather than a dependency: this is a ~50
// line client and does not need axios or got to make one JSON request.

export class ApiError extends Error {
  constructor(status, code, message) {
    super(`cogs: ${status} ${code}: ${message}`);
    this.status = status;
    this.code = code;
  }
}

export class Client {
  constructor(baseUrl, token = "") {
    this.baseUrl = baseUrl.replace(/\/$/, "");
    this.token = token;
  }

  async #request(method, path, body) {
    const headers = { "Content-Type": "application/json" };
    if (this.token) headers["Authorization"] = `Bearer ${this.token}`;

    const resp = await fetch(this.baseUrl + path, {
      method,
      headers,
      body: body !== undefined ? JSON.stringify(body) : undefined,
    });

    const text = await resp.text();
    const data = text ? JSON.parse(text) : {};

    if (!resp.ok) {
      const err = data.error || {};
      throw new ApiError(resp.status, err.code || "unknown", err.message || resp.statusText);
    }
    return data;
  }

  enqueue(queue, payload, idempotencyKey = "") {
    return this.#request("POST", "/v1/jobs", {
      queue,
      payload,
      idempotency_key: idempotencyKey,
    });
  }

  claim(queues, batch, waitMs, workerId) {
    return this.#request("POST", "/v1/jobs/claim", {
      queues,
      batch,
      wait_ms: waitMs,
      worker_id: workerId,
    });
  }

  // Rejects with ApiError(status=409) when the lease is already gone.
  // Callers must treat that as "stop working," not a retryable failure --
  // see PROTOCOL.md, "Why a 409 on renew means stop, not retry".
  renew(queue, jobId) {
    return this.#request("POST", `/v1/jobs/${jobId}/renew`, { queue });
  }

  ack(queue, ids) {
    return this.#request("POST", "/v1/jobs/ack", { queue, ids });
  }

  fail(queue, jobId, error, attempt) {
    return this.#request("POST", `/v1/jobs/${jobId}/fail`, {
      queue,
      error,
      attempt,
    });
  }
}
