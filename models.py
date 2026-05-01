"""SQLAlchemy models for the 5-stage pipeline."""

import uuid
from datetime import datetime

from flask_sqlalchemy import SQLAlchemy

db = SQLAlchemy()


class Job(db.Model):
    __tablename__ = "jobs"

    id = db.Column(db.String, primary_key=True, default=lambda: str(uuid.uuid4()))
    status = db.Column(db.String, nullable=False, default="pending")
    current_stage = db.Column(db.Integer, nullable=False, default=0)
    failed_stage = db.Column(db.Integer, nullable=True)
    error = db.Column(db.Text, nullable=True)
    created_at = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)
    updated_at = db.Column(
        db.DateTime, nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow
    )

    top_words = db.relationship("TopWord", backref="job", lazy=True)


class TopWord(db.Model):
    __tablename__ = "top_words"

    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    job_id = db.Column(db.String, db.ForeignKey("jobs.id"), nullable=False)
    word = db.Column(db.Text, nullable=False)
    count = db.Column(db.Integer, nullable=False)
