"""Pipeline stage functions executed by the RQ worker.

Each stage:
  1. Updates the job's current_stage + status to "running" in Postgres.
  2. Does its transformation work (read input, write output file).
  3. On success: enqueues the next stage (or marks the job completed at stage 5).
  4. On any exception: marks the job failed with failed_stage + error message.
     The worker process itself never crashes.
"""

import json
import os
import re
import time

import psycopg2
from redis import Redis
from rq import Queue

STOPWORDS = {
    "the", "a", "an", "and", "or", "but", "if", "then", "of",
    "in", "on", "at", "to", "for", "with", "by", "is", "are",
    "was", "were", "be", "been", "being", "this", "that",
}


# ---------------------------------------------------------------------------
# Database helpers (raw psycopg2 so the worker needs no Flask context)
# ---------------------------------------------------------------------------

def _get_conn():
    return psycopg2.connect(os.environ["DATABASE_URL"])


def _update_job(job_id, **fields):
    """Update arbitrary columns on a jobs row plus updated_at."""
    conn = _get_conn()
    try:
        cur = conn.cursor()
        assignments = ", ".join(f"{col} = %s" for col in fields)
        assignments += ", updated_at = NOW()"
        values = list(fields.values()) + [job_id]
        cur.execute(f"UPDATE jobs SET {assignments} WHERE id = %s", values)
        conn.commit()
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Main entry point called by RQ
# ---------------------------------------------------------------------------

def run_stage(job_id: str, stage_num: int, text: str = None):
    """Run one pipeline stage.  Enqueues stage+1 on success, or marks failed."""

    # Immediately signal that this stage is active.
    _update_job(job_id, status="running", current_stage=stage_num)

    job_dir = f"/data/{job_id}"
    os.makedirs(job_dir, exist_ok=True)

    try:
        _execute_stage(job_id, stage_num, job_dir, text)

        # Chain to the next stage or mark complete.
        if stage_num < 5:
            redis_conn = Redis.from_url(os.environ["REDIS_URL"])
            q = Queue("pipeline", connection=redis_conn)
            q.enqueue(run_stage, job_id, stage_num + 1)
        else:
            _update_job(job_id, status="completed", current_stage=5)

    except Exception as exc:
        _update_job(
            job_id,
            status="failed",
            current_stage=stage_num,
            failed_stage=stage_num,
            error=str(exc),
        )


def _execute_stage(job_id: str, stage_num: int, job_dir: str, text: str):
    """Perform the actual transformation for a given stage."""

    # Short pause so rapid polling can observe each current_stage value.
    time.sleep(0.15)

    if stage_num == 1:
        # Write raw text to disk.
        with open(f"{job_dir}/stage1.txt", "w", encoding="utf-8") as fh:
            fh.write(text)

    elif stage_num == 2:
        # Lowercase the entire text.
        with open(f"{job_dir}/stage1.txt", "r", encoding="utf-8") as fh:
            content = fh.read()
        with open(f"{job_dir}/stage2.txt", "w", encoding="utf-8") as fh:
            fh.write(content.lower())

    elif stage_num == 3:
        # Tokenize: split on whitespace + punctuation → JSON list of tokens.
        with open(f"{job_dir}/stage2.txt", "r", encoding="utf-8") as fh:
            content = fh.read()
        tokens = re.findall(r"\w+", content)
        with open(f"{job_dir}/stage3.json", "w", encoding="utf-8") as fh:
            json.dump(tokens, fh)

    elif stage_num == 4:
        # Remove stopwords → JSON list.
        with open(f"{job_dir}/stage3.json", "r", encoding="utf-8") as fh:
            tokens = json.load(fh)
        filtered = [t for t in tokens if t not in STOPWORDS]
        with open(f"{job_dir}/stage4.json", "w", encoding="utf-8") as fh:
            json.dump(filtered, fh)

    elif stage_num == 5:
        # Compute word frequencies → JSON object {word: count}.
        with open(f"{job_dir}/stage4.json", "r", encoding="utf-8") as fh:
            tokens = json.load(fh)

        if not tokens:
            raise ValueError("No tokens remaining after stopword removal")

        freq: dict[str, int] = {}
        for token in tokens:
            freq[token] = freq.get(token, 0) + 1

        with open(f"{job_dir}/stage5.json", "w", encoding="utf-8") as fh:
            json.dump(freq, fh)

        # Persist top-5 most frequent words to the database.
        top5 = sorted(freq.items(), key=lambda kv: kv[1], reverse=True)[:5]
        conn = _get_conn()
        try:
            cur = conn.cursor()
            for word, count in top5:
                cur.execute(
                    "INSERT INTO top_words (job_id, word, count) VALUES (%s, %s, %s)",
                    (job_id, word, count),
                )
            conn.commit()
        finally:
            conn.close()
