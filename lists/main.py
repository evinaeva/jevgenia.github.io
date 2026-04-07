"""
Cloud Function: ES Lists API proxy
Endpoints:
  GET  /get-upload-url?session_id=XXX  → {url, gcs_path}
  POST /trigger-import                  → {ok: true}
    body: {"session_id": "XXX", "gcs_path": "uploads/XXX/input.zip"}

Required env vars:
  GITHUB_TOKEN    Fine-grained PAT with actions:write
  GITHUB_REPO     e.g. evinaeva/jevgenia.github.io
  GCS_BUCKET      e.g. es-lists-jevgenia
  GCS_SA_KEY_JSON Service account JSON key (full content)
"""
import os, json, re, datetime
import functions_framework
from flask import jsonify
from google.cloud import storage
from google.oauth2 import service_account
import requests

GITHUB_TOKEN = os.environ["GITHUB_TOKEN"]
GITHUB_REPO  = os.environ.get("GITHUB_REPO", "evinaeva/jevgenia.github.io")
GCS_BUCKET   = os.environ["GCS_BUCKET"]
GCS_SA_KEY   = json.loads(os.environ["GCS_SA_KEY_JSON"])

ALLOWED_ORIGINS = {
    "https://lists.jevgenia.com",
    "https://evinaeva.github.io",
    "http://localhost:8080",
    "http://localhost",
}

_SESSION_RE = re.compile(r'^[0-9a-f\-]{8,64}$')
_GCSPATH_RE = re.compile(r'^uploads/[0-9a-f\-]{8,64}/input\.zip$')


def _cors(origin: str) -> dict:
    allowed = origin if origin in ALLOWED_ORIGINS else "https://lists.jevgenia.com"
    return {
        "Access-Control-Allow-Origin": allowed,
        "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
        "Access-Control-Allow-Headers": "Content-Type",
        "Access-Control-Max-Age": "3600",
    }


@functions_framework.http
def es_api(request):
    origin = request.headers.get("Origin", "")
    hdrs = _cors(origin)

    if request.method == "OPTIONS":
        return ("", 204, hdrs)

    path = request.path.rstrip("/")

    if path == "/get-upload-url" and request.method == "GET":
        return _get_upload_url(request, hdrs)
    if path == "/trigger-import" and request.method == "POST":
        return _trigger_import(request, hdrs)

    return (jsonify({"error": "not found"}), 404, hdrs)


def _get_upload_url(request, hdrs):
    session_id = request.args.get("session_id", "").strip()
    if not _SESSION_RE.match(session_id):
        return (jsonify({"error": "invalid session_id"}), 400, hdrs)

    creds = service_account.Credentials.from_service_account_info(
        GCS_SA_KEY,
        scopes=["https://www.googleapis.com/auth/cloud-platform"],
    )
    client = storage.Client(credentials=creds, project=GCS_SA_KEY["project_id"])
    blob_path = f"uploads/{session_id}/input.zip"
    blob = client.bucket(GCS_BUCKET).blob(blob_path)

    signed_url = blob.generate_signed_url(
        version="v4",
        expiration=datetime.timedelta(hours=1),
        method="PUT",
        content_type="application/zip",
    )
    return (jsonify({"url": signed_url, "gcs_path": blob_path}), 200, hdrs)


def _trigger_import(request, hdrs):
    data = request.get_json(silent=True) or {}
    session_id = str(data.get("session_id", "")).strip()
    gcs_path   = str(data.get("gcs_path",   "")).strip()

    if not _SESSION_RE.match(session_id) or not _GCSPATH_RE.match(gcs_path):
        return (jsonify({"error": "invalid parameters"}), 400, hdrs)

    url = (
        f"https://api.github.com/repos/{GITHUB_REPO}"
        f"/actions/workflows/es-import.yml/dispatches"
    )
    r = requests.post(
        url,
        headers={
            "Authorization": f"token {GITHUB_TOKEN}",
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
        },
        json={"ref": "main", "inputs": {"gcs_path": gcs_path, "log_id": session_id}},
        timeout=30,
    )

    if r.status_code not in (200, 204):
        return (jsonify({"error": f"GitHub API error {r.status_code}"}), 502, hdrs)

    return (jsonify({"ok": True}), 200, hdrs)
