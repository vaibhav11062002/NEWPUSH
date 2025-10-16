"""
WSGI config for backend project.

It exposes the WSGI callable as a module-level variable named ``application``.

For more information on this file, see
https://docs.djangoproject.com/en/5.1/howto/deployment/wsgi/
"""

import os
from google.cloud import storage

def download_sqlite_from_gcs():
    gcs_path = os.getenv("GCS_DB_PATH")
    local_path = os.getenv("DB_PATH", "/tmp/db.sqlite3")
    if not gcs_path:
        raise Exception("GCS_DB_PATH environment variable not set")

    path_without_scheme = gcs_path[5:]  # remove 'gs://'
    bucket_name, blob_name = path_without_scheme.split('/', 1)

    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_name)

    if not os.path.exists(local_path):
        os.makedirs(os.path.dirname(local_path), exist_ok=True)
        print(f"Downloading SQLite DB from {gcs_path} to {local_path} ...")
        blob.download_to_filename(local_path)
        print("Download complete.")

# Download the SQLite DB file before initializing Django
download_sqlite_from_gcs()

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'backend.settings')

from django.core.wsgi import get_wsgi_application
application = get_wsgi_application()
