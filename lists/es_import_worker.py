#!/usr/bin/env python3
"""
GitHub Actions worker: downloads ZIP/RAR/TGZ from GCS, extracts CSVs,
uploads each to GCS (public), creates ES list, triggers import.

Required env vars:
  ES_API_KEY       ExpertSender API key
  ES_BASE_URL      ExpertSender API base URL (e.g. https://api6.esv2.com/v2/Api)
  GCS_BUCKET       GCS bucket name
  GCS_SA_KEY_JSON  Service account JSON key (full content)
  LOG_ID           Unique session ID
  GCS_UPLOAD_PATH  Path of uploaded archive in GCS (e.g. uploads/LOG_ID/BNG-30632.zip)
"""
import os, re, sys, json, time, zipfile, tarfile, io, tempfile
import requests
from google.cloud import storage as gcs_lib
from google.oauth2 import service_account

ES_API_KEY      = os.environ["ES_API_KEY"]
ES_BASE_URL     = os.environ["ES_BASE_URL"].rstrip("/")
GCS_BUCKET      = os.environ["GCS_BUCKET"]
GCS_SA_KEY_JSON = os.environ["GCS_SA_KEY_JSON"]
LOG_ID          = os.environ["LOG_ID"]
GCS_UPLOAD_PATH = os.environ["GCS_UPLOAD_PATH"]

# Derive archive name (without extension) from GCS_UPLOAD_PATH
_arc_basename = os.path.basename(GCS_UPLOAD_PATH)
if _arc_basename.lower().endswith('.tar.gz'):
    ARCHIVE_NAME = _arc_basename[:-7]
else:
    ARCHIVE_NAME = os.path.splitext(_arc_basename)[0]

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
_log_push_count = 0

def _push_log(status: str):
    global _log_push_count
    # Write to a new sequential file each time to avoid needing storage.objects.delete
    blob = bucket_obj.blob(f"logs/{LOG_ID}/{_log_push_count}.json")
    blob.upload_from_string(
        json.dumps({"status": status, "logs": log_entries}),
        content_type="application/json",
        timeout=15,
    )
    _log_push_count += 1

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
    """Returns '<archive_name>_<csv_basename_without_ext>'."""
    base = fn[:-4] if fn.lower().endswith(".csv") else fn
    return f"{ARCHIVE_NAME}_{base}"

def _is_valid_csv(name: str) -> bool:
    bn = os.path.basename(name)
    return (
        name.lower().endswith(".csv")
        and not name.startswith("__MACOSX")
        and bool(bn)
        and not bn.startswith(".")
    )

def extract_csvs(data: bytes, filename: str) -> dict:
    """
    Extract all CSV files from an archive (zip, rar, tgz/tar.gz).
    Returns {basename: bytes}. Handles nested folders at any depth.
    """
    name_lower = filename.lower()
    result = {}

    if name_lower.endswith(".tar.gz") or name_lower.endswith(".tgz"):
        with tarfile.open(fileobj=io.BytesIO(data), mode="r:gz") as tf:
            for member in tf.getmembers():
                if member.isfile() and _is_valid_csv(member.name):
                    f = tf.extractfile(member)
                    if f:
                        result[os.path.basename(member.name)] = f.read()

    elif name_lower.endswith(".rar"):
        import rarfile
        tmp_fd, tmp_path = tempfile.mkstemp(suffix=".rar")
        try:
            with os.fdopen(tmp_fd, "wb") as f:
                f.write(data)
            with rarfile.RarFile(tmp_path) as rf:
                for name in rf.namelist():
                    if _is_valid_csv(name):
                        result[os.path.basename(name)] = rf.read(name)
        finally:
            os.unlink(tmp_path)

    else:  # .zip (default)
        with zipfile.ZipFile(io.BytesIO(data)) as zf:
            for name in zf.namelist():
                if _is_valid_csv(name):
                    result[os.path.basename(name)] = zf.read(name)

    return result

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
      <OptInMode>SingleOptIn</OptInMode>
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
      <Mode>AddNew</Mode>
      <Delimiter>,</Delimiter>
      <Encoding>UTF-8</Encoding>
      <AllowImportingUnsubscribedEmail>false</AllowImportingUnsubscribedEmail>
      <AllowImportingRemovedByUiEmail>false</AllowImportingRemovedByUiEmail>
      <CheckAllListsForUnsubscribes>true</CheckAllListsForUnsubscribes>
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
    log(f"Starting ES import process (archive: {ARCHIVE_NAME})")

    arc_url = f"https://storage.googleapis.com/{GCS_BUCKET}/{GCS_UPLOAD_PATH}"
    log("Downloading archive…")
    try:
        r = requests.get(arc_url, timeout=180)
        r.raise_for_status()
        arc_bytes = r.content
        log(f"Archive downloaded: {len(arc_bytes):,} bytes")
    except Exception as e:
        log(f"Download failed: {e}", "error")
        finish("error")
        sys.exit(1)

    arc_filename = os.path.basename(GCS_UPLOAD_PATH)
    try:
        csv_map = extract_csvs(arc_bytes, arc_filename)
    except Exception as e:
        log(f"Failed to open archive: {e}", "error")
        finish("error")
        sys.exit(1)

    if not csv_map:
        log("No CSV files found in archive", "error")
        finish("error")
        sys.exit(1)

    csv_items = list(csv_map.items())  # [(basename, bytes), ...]
    log(f"Found {len(csv_items)} CSV file(s)")
    errors = []

    for i, (basename, csv_data) in enumerate(csv_items, 1):
        log(f"[{i}/{len(csv_items)}] {basename}")

        try:
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
        log(f"\n{len(errors)} error(s) out of {len(csv_items)} file(s):", "warn")
        for err in errors:
            log(f"  {err['file']} [{err['step']}]: {err['error']}", "error")
        finish("partial")
    else:
        log(f"\nAll {len(csv_items)} file(s) imported successfully!")
        finish("done")

if __name__ == "__main__":
    main()
