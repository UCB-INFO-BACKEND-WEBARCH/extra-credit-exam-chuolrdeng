"""RQ worker entrypoint.

Starts listening on the "pipeline" queue. The API container is responsible
for running db.create_all() at startup; the worker has no Flask dependency.
"""

import os

from redis import Redis
from rq import Worker

if __name__ == "__main__":
    redis_conn = Redis.from_url(os.environ["REDIS_URL"])
    Worker(["pipeline"], connection=redis_conn).work()
