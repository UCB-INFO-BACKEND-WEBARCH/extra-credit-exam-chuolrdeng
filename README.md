[![Review Assignment Due Date](https://classroom.github.com/assets/deadline-readme-button-22041afd0340ce965d47ae6ef1cefeee28c7c493a6346c4f15d667ab976d596c.svg)](https://classroom.github.com/a/DzbHDzlF)
# Extra Credit Exam: 5-Stage Data Pipeline

**Duration:** 50 minutes, in-class
**AI tools:** Allowed
**Grading:** All-or-nothing — pass = 10 bonus points applicable to any single assignment, fail = 0

---

## What you're building

A backend that ingests text and runs it through a **5-stage pipeline**, where each stage runs sequentially as a background job, reads its input from the previous stage's output file, and updates job status in Postgres along the way. A status endpoint reports progress.

You will deliver one repo with a working `docker-compose up --build` that satisfies every requirement below. The grader is automated and strict — every check must pass or the entire submission fails.

---

## What the starter gives you

The starter is **deliberately minimal**. Already done:
- `docker-compose.yml` declares 4 services (`api`, `worker`, `db`, `redis`) and a **named volume `pipeline_data` mounted in both `api` and `worker` at `/data`**. You read and write stage output files here; both containers see the same files.
- `Dockerfile`, `requirements.txt`, empty Flask app (`app.py`), empty worker entrypoint (`worker.py`), empty models file (`models.py`)
- That's it. Every endpoint, model, stage, and config wiring is yours.

---

## Required endpoints

### `POST /jobs`

Accepts a JSON payload `{"text": "..."}` and starts a new pipeline job.

**Response:** `202 Accepted`
```json
{ "job_id": "<uuid-or-int>" }
```

The job runs asynchronously through the 5 stages. The endpoint must return promptly (< 1 second); do not run stages inline.

### `GET /jobs/<id>`

Returns the current state of a job.

**Response (success):** `200 OK`
```json
{
    "job_id": "<id>",
    "status": "running",          // pending | running | completed | failed
    "current_stage": 3,            // 1..5; the stage currently running or the last completed stage
    "failed_stage": null,          // null unless status == "failed"
    "error": null                  // null unless status == "failed"
}
```

**Response (not found):** `404 Not Found`

### `GET /health`

Returns liveness + dependency status.

**Response:** `200 OK`
```json
{
    "status": "ok",
    "db": "up",
    "redis": "up",
    "volume_writable": true
}
```

`db` and `redis` must reflect actual connectivity (try a `SELECT 1` and a `redis.ping()`). `volume_writable` must reflect whether `/data` is writable from this process.

---

## The 5 stages

Stage `i` reads from `/data/<job_id>/stage{i-1}.<ext>` (except stage 1, which reads from the POST payload) and writes to `/data/<job_id>/stage{i}.<ext>`. **Each stage runs as a separate background job, sequentially — stage `i+1` must not start until stage `i` is fully done.**

| Stage | Input | Output file | What it does |
|-------|-------|-------------|--------------|
| 1 | `text` from POST body | `/data/<id>/stage1.txt` | Write raw text to disk |
| 2 | `stage1.txt` | `/data/<id>/stage2.txt` | Lowercase the entire text |
| 3 | `stage2.txt` | `/data/<id>/stage3.json` | Tokenize: split on whitespace + punctuation; write JSON list of word tokens |
| 4 | `stage3.json` | `/data/<id>/stage4.json` | Remove stopwords (use the list in `STOPWORDS` below); write JSON list |
| 5 | `stage4.json` | `/data/<id>/stage5.json` | Compute frequencies; write JSON object `{word: count}`. Also save the **top 5 most frequent words** to the database in a `top_words` table. |

The grader will verify all 5 files exist with correct content **and** the database has the top-5 words for each completed job.

### `STOPWORDS` (use exactly this list)

```python
STOPWORDS = {"the", "a", "an", "and", "or", "but", "if", "then", "of",
             "in", "on", "at", "to", "for", "with", "by", "is", "are",
             "was", "were", "be", "been", "being", "this", "that"}
```

### Required database schema (minimum)

```
jobs:
  id            primary key (uuid string or int)
  status        text  ('pending' | 'running' | 'completed' | 'failed')
  current_stage int   (1..5)
  failed_stage  int   nullable
  error         text  nullable
  created_at    timestamp
  updated_at    timestamp

top_words:
  job_id  fk -> jobs.id
  word    text
  count   int
```

You may add columns; do not remove the ones above.

---

## The four hard parts (read this section twice)

These are the points where the grader will catch shortcuts. The spec calls them out so you don't waste time discovering them — but you still have to implement them correctly.

### 1. Concurrent jobs

The grader will fire **3 `POST /jobs` requests within 2 seconds** with different payloads. All three must run their own pipelines without cross-contaminating files (each must use its own `/data/<job_id>/` directory) or DB rows. Each must complete with its own correct top-5.

### 2. Strict failure semantics

If any stage raises an exception (for instance, stage 5 receiving an empty list because every word was a stopword), the worker **must catch it and update the job to**:
- `status = "failed"`
- `failed_stage = <the stage number that raised>`
- `error = "<the exception message>"`

The worker process must NOT crash. It must continue accepting new jobs after a failure. The grader will submit a payload of all stopwords (`"the and a or but"`) and expect a failed-job response — not a 500, not a hung "running" forever.

### 3. Health and readiness gating

Two pieces:
- `GET /health` must return the JSON shape above with **real** dependency checks (actually query DB, actually ping Redis, actually try writing to `/data`).
- `docker-compose.yml` must use `depends_on: condition: service_healthy` on `api` and `worker` so they wait for `db` and `redis` to be healthy before starting. This requires you to **add `healthcheck` blocks** to the `db` and `redis` services. The grader hits `/health` immediately after `docker-compose up` finishes — if your services raced and crashed, you fail.

### 4. Sequential stage chaining done correctly

Stages must run **one at a time, in order**. Two failure modes the grader will catch:
- **All-at-once enqueue:** if you push all 5 stages into the queue simultaneously, they will run out of order and stage 2 will read a missing file. Don't do this.
- **Synchronous chain inside a single job:** if a single rq job runs all 5 stages back-to-back, your status endpoint can never report `current_stage: 3` while stage 3 is running. The grader polls rapidly during execution and asserts that `current_stage` actually advances 1 → 2 → 3 → 4 → 5 over time.

The intended pattern is: stage `i` finishes by enqueueing stage `i+1` (or use rq's `depends_on` argument). Either works.

---

## What the grader does

A single command: `docker-compose up --build`, then HTTP requests. The grader checks:

1. `docker-compose up --build` exits 0 and all containers stay healthy
2. `GET /health` returns the expected JSON within 5 seconds
3. Three concurrent `POST /jobs` calls all return 202 with a `job_id` in < 1 second each
4. Polling each job's `GET /jobs/<id>` shows `current_stage` advancing through 1, 2, 3, 4, 5 (the grader will catch all-at-once parallel enqueue here)
5. Each job reaches `status: "completed"` with `current_stage: 5` within 30 seconds
6. For each completed job, all 5 stage files exist on the volume with correct content
7. Each completed job has top-5 word entries in `top_words`
8. A 4th job with payload `{"text": "the and a or but"}` reaches `status: "failed"` with `failed_stage` set and a non-null `error`. The worker is still alive after this.

**Any single check fails → 0/10.**

---

## Time budget reality check

This is **hard, even with AI assistance**. Realistic budget for a strong AI-assisted student:
- Read this spec: 5 min
- Models + DB init: 5 min
- API endpoints: 10 min
- 5 stages + chaining: 15 min
- Healthchecks + depends_on: 5 min
- Failure handling: 5 min
- Verify against the spec, fix edge cases: 5 min
- **Total: 50 min, with no margin.**

Most students will not finish. That's expected — this is extra credit.

---

## Submission

Push to your GitHub Classroom repo before the timer ends. The grader runs `docker-compose up --build` from the repo root.
