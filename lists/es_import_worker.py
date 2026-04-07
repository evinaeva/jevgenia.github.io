#!/usr/bin/env python3
"""
GitHub Actions worker: downloads ZIP from GCS, extracts CSVs,
uploads each to GCS (public), creates ES list, triggers import.

Required env vars:
  ES_API_KEY       ExpertSender API key
  ES_BASE_URL      ExpertSender API base URL (e.g. https://api6.esv2.com/v2/Api)
  GCS_BUCKET       GCS bucket name
  GCS_SA_KEY_JSON  Service account JSON key (full content)
  LOG_ID           Unique session ID
  GCS_UPLOAD_PATH  Path of uploaded ZIP in GCS (e.g. uploads/LOG_ID/input.zip)
"""
import os, re, sys, json, time, zipfile, io
import requests
from google.cloud import storage as gcs_lib
from google.oauth2 import service_account

ES_API_KEY      = os.environ["ES_API_KEY"]
ES_BASE_URL     = os.environ["ES_BASE_URL"].rstrip("/")
GCS_BUCKET      = os.environ["GCS_BUCKET"]
GCS_SA_KEY_JSON = os.environ["GCS_SA_KEY_JSON"]
LOG_ID          = os.environ["LOG_ID"]
GCS_UPLOAD_PATH = os.environ["GCS_UPLOAD_PATH"]

SUPPORTED_LANGS = {
    "en-US","pl-PL","ru-RU","zh-Hans","pt-BR","es-ES","it-IT","fr-FR","de-DE","cs-CZ",
    "nl-NL","hu-HU","ro-RO","sk-SK","lt-LT","lv-LV","et-EE","hr-HR","uk-UA","sl-SI",
    "bg-BG","el-GR","sr-Latn","ja-JP"
}

# --- GCS client ---
_sa_info = json.loads(GCS_SA_KEY_JSON)
_creds   = service_account.Credentials.from_service_account_info(_sa_info)
gcs_client = gcs_lib.Client(credentials=_creds, project=_sa_info["project_id"])
bucket_obj = gcs_client.bucket(GCS_BUCKET)

log_entries: list = []

def _push_log(status: str):
    blob = bucket_obj.blob(f"logs/{LOG_ID}.json")
    blob.upload_from_string(
        json.dumps({"status": status, "logs": log_entries}),
        content_type="application/json",
        timeout=15,
    )

def log(msg: str, level: str = "info"):
    entry = {"t": time.strftime("%H:%M:%S"), "msg": msg, "level": level}
    log_entries.append(entry)
    print(f"[{entry['t']}] {msg}", flush=True)
    try:
        _push_log("running")
    except Exception as e:
        print(f"[warn] log push failed: {e}", flush=True)

def finish(status: str):
    try:
        _push_log(status)
    except Exception as e:
        print(f"[warn] final log push failed: {e}", flush=True)

# --- Helpers ---
def lang_from_filename(fn: str) -> str:
    m = re.search(r"_([a-z]{2})_([A-Z]{2})[_.]", fn)
    if m:
        cand = f"{m.group(1)}-{m.group(2)}"
        if cand in SUPPORTED_LANGS:
            return cand
        if m.group(1) == "en":
            return "en-US"
    return "en-US"

def list_name_from_filename(fn: str) -> str:
    return fn[:-4] if fn.lower().endswith(".csv") else fn

def _xml(inner: str) -> bytes:
    return f"""<ApiRequest xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
  <ApiKey>{ES_API_KEY}</ApiKey>
  <Data>
{inner}
  </Data>
</ApiRequest>""".encode("utf-8")

def _post_es(path: str, xml: bytes, timeout: int = 60) -> requests.Response:
    url = f"{ES_BASE_URL}/{path.lstrip('/')}?apiKey={ES_API_KEY}"
    return requests.post(url, data=xml, headers={"Content-Type": "application/xml"}, timeout=timeout)

def _parse_int(text: str, tag: str = "Data"):
    m = re.search(fr"<{tag}>(\d+)</{tag}>", text)
    return int(m.group(1)) if m else None

def create_es_list(name: str, lang: str) -> int:
    xml = _xml(f"""    <GeneralSettings>
      <Name><![CDATA[{name}]]></Name>
      <Language>{lang}</Language>
    </GeneralSettings>""")
    r = _post_es("Lists", xml)
    if r.status_code >= 300:
        raise RuntimeError(f"HTTP {r.status_code}: {r.text[:300]}")
    lid = _parse_int(r.text)
    if lid is None:
        raise RuntimeError(f"Cannot parse ListId: {r.text[:300]}")
    return lid

def trigger_es_import(list_id: int, list_name: str, file_url: str) -> int:
    mapping = """      <Mapping>
        <Column><Number>0</Number><Field>Email</Field></Column>
        <Column><Number>1</Number><Field>username</Field></Column>
        <Column><Number>2</Number><Field>Firstname</Field></Column>
        <Column><Number>3</Number><Field>favmodel_username</Field></Column>
        <Column><Number>4</Number><Field>favmodel_displayname</Field></Column>
      </Mapping>"""
    xml = _xml(f"""    <Source>
      <Url><![CDATA[{file_url}]]></Url>
    </Source>
    <Target>
      <Name><![CDATA[Import {list_name}]]></Name>
      <SubscriberList>{list_id}</SubscriberList>
    </Target>
    <ImportSetup>
      <Mode>AddAndUpdate</Mode>
      <Delimiter>,</Delimiter>
      <Encoding>UTF-8</Encoding>
{mapping}
    </ImportSetup>""")
    r = _post_es("ImportToListTasks", xml, timeout=120)
    if r.status_code >= 300:
        raise RuntimeError(f"HTTP {r.status_code}: {r.text[:300]}")
    tid = _parse_int(r.text)
    if tid is None:
        raise RuntimeError(f"Cannot parse TaskId: {r.text[:300]}")
    return tid

# --- Main ---
def main():
    log("Starting ES import process")

    zip_url = f"https://storage.googleapis.com/{GCS_BUCKET}/{GCS_UPLOAD_PATH}"
    log("Downloading ZIP…")
    try:
        r = requests.get(zip_url, timeout=180)
        r.raise_for_status()
        zip_bytes = r.content
        log(f"ZIP downloaded: {len(zip_bytes):,} bytes")
    except Exception as e:
        log(f"Download failed: {e}", "error")
        finish("error")
        sys.exit(1)

    try:
        zf = zipfile.ZipFile(io.BytesIO(zip_bytes))
        csv_files = [
            n for n in zf.namelist()
            if n.lower().endswith(".csv")
            and not n.startswith("__MACOSX")
            and os.path.basename(n)
            and not os.path.basename(n).startswith(".")
        ]
    except Exception as e:
        log(f"Failed to open ZIP: {e}", "error")
        finish("error")
        sys.exit(1)

    if not csv_files:
        log("No CSV files found in ZIP", "error")
        finish("error")
        sys.exit(1)

    log(f"Found {len(csv_files)} CSV file(s)")
    errors = []

    for i, csv_path in enumerate(csv_files, 1):
        basename = os.path.basename(csv_path)
        log(f"[{i}/{len(csv_files)}] {basename}")

        try:
            csv_data = zf.read(csv_path)
            gcs_obj = f"imports/{LOG_ID}/{basename}"
            blob = bucket_obj.blob(gcs_obj)
            blob.upload_from_string(csv_data, content_type="text/csv", timeout=120)
            public_url = f"https://storage.googleapis.com/{GCS_BUCKET}/{gcs_obj}"
            log(f"  → uploaded to GCS ({len(csv_data):,} bytes)")
        except Exception as e:
            log(f"  GCS upload failed: {e}", "error")
            errors.append({"file": basename, "step": "gcs_upload", "error": str(e)})
            continue

        list_name = list_name_from_filename(basename)
        lang = lang_from_filename(basename)
        try:
            list_id = create_es_list(list_name, lang)
            log(f"  → ES list created: id={list_id}, lang={lang}")
        except Exception as e:
            log(f"  Create list failed: {e}", "error")
            errors.append({"file": basename, "step": "create_list", "error": str(e)})
            continue

        try:
            task_id = trigger_es_import(list_id, list_name, public_url)
            log(f"  → Import task started: task_id={task_id}")
        except Exception as e:
            log(f"  Import trigger failed: {e}", "error")
            errors.append({"file": basename, "step": "import", "error": str(e)})
            continue

        time.sleep(0.3)

    if errors:
        log(f"\n{len(errors)} error(s) out of {len(csv_files)} file(s):", "warn")
        for err in errors:
            log(f"  {err['file']} [{err['step']}]: {err['error']}", "error")
        finish("partial")
    else:
        log(f"\nAll {len(csv_files)} file(s) imported successfully!")
        finish("done")

if __name__ == "__main__":
    main()
