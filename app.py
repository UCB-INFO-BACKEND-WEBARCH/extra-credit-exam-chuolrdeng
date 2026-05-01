"""Flask API for the 5-stage data pipeline."""

import os
import uuid

from flask import Flask, jsonify, request
from redis import Redis
from rq import Queue
from sqlalchemy import text

from models import Job, db


def create_app():
    app = Flask(__name__)
    app.config["SQLALCHEMY_DATABASE_URI"] = os.environ["DATABASE_URL"]
    app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False
    db.init_app(app)

    with app.app_context():
        try:
            db.create_all()
        except Exception:
            # Retry once — handles rare transient errors on first boot
            db.session.rollback()
            db.create_all()

    # ------------------------------------------------------------------
    # POST /jobs — accept text, create a job row, enqueue stage 1
    # ------------------------------------------------------------------
    @app.route("/jobs", methods=["POST"])
    def create_job():
        data = request.get_json(force=True, silent=True) or {}
        input_text = data.get("text", "")

        job_id = str(uuid.uuid4())
        job = Job(id=job_id, status="pending", current_stage=0)
        db.session.add(job)
        db.session.commit()

        redis_conn = Redis.from_url(os.environ["REDIS_URL"])
        q = Queue("pipeline", connection=redis_conn)
        # Import here to keep the module-level namespace clean and avoid
        # any potential circular-import edge cases at import time.
        from pipeline_stages import run_stage
        q.enqueue(run_stage, job_id, 1, input_text)

        return jsonify({"job_id": job_id}), 202

    # ------------------------------------------------------------------
    # GET /jobs/<id> — return current job state
    # ------------------------------------------------------------------
    @app.route("/jobs/<job_id>", methods=["GET"])
    def get_job(job_id):
        job = db.session.get(Job, job_id)
        if job is None:
            return jsonify({"error": "not found"}), 404

        return jsonify(
            {
                "job_id": job.id,
                "status": job.status,
                "current_stage": job.current_stage,
                "failed_stage": job.failed_stage,
                "error": job.error,
            }
        ), 200

    # ------------------------------------------------------------------
    # GET /health — liveness + real dependency checks
    # ------------------------------------------------------------------
    @app.route("/health", methods=["GET"])
    def health():
        # Check Postgres.
        db_status = "up"
        try:
            db.session.execute(text("SELECT 1"))
        except Exception:
            db_status = "down"

        # Check Redis.
        redis_status = "up"
        try:
            Redis.from_url(os.environ["REDIS_URL"]).ping()
        except Exception:
            redis_status = "down"

        # Check shared volume is writable.
        volume_writable = False
        try:
            probe = "/data/.health_probe"
            with open(probe, "w") as fh:
                fh.write("ok")
            os.remove(probe)
            volume_writable = True
        except Exception:
            volume_writable = False

        return jsonify(
            {
                "status": "ok",
                "db": db_status,
                "redis": redis_status,
                "volume_writable": volume_writable,
            }
        ), 200

    return app


app = create_app()

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, threaded=True, use_reloader=False)
