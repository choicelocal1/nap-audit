web: gunicorn --bind 0.0.0.0:$PORT wsgi:app
worker: celery -A tasks worker --loglevel=info --concurrency=2

