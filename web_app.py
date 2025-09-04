from __future__ import annotations

import io
import os
import threading
from pathlib import Path
import shutil
import tempfile
from typing import Optional

from flask import Flask, render_template_string, request, send_file, abort

from cmd_job_builder import load_config, run_builder
import yaml


app = Flask(__name__)

TEMPLATE = """
<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>CMD Job Builder</title>
  <style>
    body { font-family: Arial, sans-serif; max-width: 720px; margin: 32px auto; }
    label { display:block; margin-top: 12px; font-weight: 600; }
    input[type=text] { width: 100%; padding: 8px; }
    .row { margin-top: 8px; }
    .btn { margin-top: 16px; padding: 10px 16px; background: #0b5; color: #fff; border: none; cursor: pointer; }
    .btn:disabled { background: #999; cursor: not-allowed; }
    .note { color: #444; font-size: 0.95em; margin-top: 8px; }
    .error { color: #b00; }
  </style>
  <script>
    function onSubmitForm(e){
      const btn = document.getElementById('runBtn');
      btn.disabled = true; btn.innerText = 'Building…';
      return true;
    }
  </script>
  </head>
  <body>
    <h1>CMD Job Builder</h1>
    <form method="POST" onsubmit="return onSubmitForm(event)">
      <label>Job Folder Name</label>
      <input type="text" name="job_folder_name" placeholder="J-25-B124 (VSR)" required />
      <div class="row">
        <label><input type="checkbox" name="sterile" checked /> Sterile</label>
      </div>
      <div class="row">
        <button id="runBtn" class="btn">Build and Download ZIP</button>
      </div>
      <div class="note">Uses config.yaml in this directory for paths; only overrides job_folder_name and sterile.</div>
      {% if error %}<div class="error">{{ error }}</div>{% endif %}
    </form>
  </body>
</html>
"""


def _run_builder_capture_zip(config_path: Path, job_folder_name: str, sterile: bool) -> Path:
    """Run the builder in a temporary workspace so nothing is written to the real job.

    Steps:
      1) Discover the original job folder under run_data_root (or one level down)
      2) Create a temp directory and copy only the CMD folder into it under the same job name
      3) Override run_data_root to the temp parent and run the builder
      4) Return the path to the created ZIP inside the temp
    """
    # Load base config
    with open(config_path, "r", encoding="utf-8") as f:
        base_cfg = yaml.safe_load(f) or {}

    run_data_root = Path(base_cfg["run_data_root"]).expanduser()
    cmd_rel = str(base_cfg.get("cmd_rel_path", "CMD"))

    # Find original job dir (direct or one-level under run_data_root)
    orig_job = run_data_root / job_folder_name
    if not orig_job.is_dir():
        cand = None
        if run_data_root.is_dir():
            for child in run_data_root.iterdir():
                if child.is_dir() and (child / job_folder_name).is_dir():
                    cand = child / job_folder_name
                    break
        if not cand:
            raise SystemExit(f"Job folder not found for web run: {orig_job} (also searched one level under {run_data_root})")
        orig_job = cand

    orig_cmd = orig_job / cmd_rel
    if not orig_cmd.is_dir():
        raise SystemExit(f"CMD directory not found in job: {orig_cmd}")

    # Build in a temp workspace
    tmp_dir = Path(tempfile.mkdtemp(prefix="cmd_web_"))
    tmp_parent = tmp_dir  # run_data_root override
    tmp_job = tmp_parent / job_folder_name
    tmp_cmd = tmp_job / cmd_rel
    tmp_cmd.mkdir(parents=True, exist_ok=True)

    # Copy only the CMD source folders to temp
    for child in orig_cmd.iterdir():
        if child.is_dir():
            shutil.copytree(child, tmp_cmd / child.name)
        else:
            # ignore stray files
            pass

    # Also copy the AM Tracker workbook that lives in the Docs folder (sibling of CMD)
    # Determine the Docs directory by taking the first segment of cmd_rel (e.g., 'docs')
    try:
        docs_segment = Path(cmd_rel).parts[0]
    except Exception:
        docs_segment = "docs"
    orig_docs = orig_job / docs_segment
    tmp_docs = tmp_job / docs_segment
    try:
        if orig_docs.is_dir():
            tmp_docs.mkdir(parents=True, exist_ok=True)
            for f in orig_docs.iterdir():
                name_low = f.name.lower()
                if f.is_file() and name_low.endswith((".xlsx", ".xlsm")) and "am tracker" in name_low:
                    shutil.copy2(f, tmp_docs / f.name)
    except Exception:
        # Non-fatal: if we can't find/copy, continue building
        pass

    # Create empty placeholder folders in temp for any non-CMD dirs under Docs in the original job
    # We only mirror top-level names under Docs (e.g., Xenix), not their contents
    docs_dir = None
    for child in orig_job.iterdir():
        if child.is_dir() and child.name.strip().lower() == "docs":
            docs_dir = child
            break
    if docs_dir is not None:
        for sub in docs_dir.iterdir():
            if sub.is_dir() and sub.name.strip().lower() != "cmd":
                # Ensure placeholder exists alongside CMD under tmp_job
                (tmp_job / sub.name).mkdir(parents=True, exist_ok=True)

    # Compose a merged config that points to temp and overrides sterile/job name
    cfg_data = dict(base_cfg)
    cfg_data["run_data_root"] = str(tmp_parent)
    cfg_data["job_folder_name"] = job_folder_name
    cfg_data["sterile"] = bool(sterile)
    # Ensure we do not set output_root (keep in temp)
    cfg_data.pop("output_root", None)

    merged_path = config_path.parent / f".web_tmp_{os.getpid()}_{threading.get_ident()}.yaml"
    with open(merged_path, "w", encoding="utf-8") as f:
        yaml.safe_dump(cfg_data, f)

    # Run the builder (not dry-run) inside temp
    cfg = load_config(merged_path)
    run_builder(cfg, verbose=True, dry_run=False)

    # Locate the zip in temp
    zip_path = tmp_parent / f"{tmp_job.name}.zip"
    if not zip_path.exists():
        raise SystemExit("ZIP not created in temporary workspace")

    # Cleanup merged config only (leave tmp dir until response streams)
    try:
        merged_path.unlink(missing_ok=True)
    except Exception:
        pass
    return zip_path


@app.route("/", methods=["GET", "POST"])
def index():
    if request.method == "GET":
        return render_template_string(TEMPLATE)

    job_folder_name = request.form.get("job_folder_name", "").strip()
    sterile_flag = request.form.get("sterile") == "on"

    if not job_folder_name:
        return render_template_string(TEMPLATE, error="Job folder name is required")

    config_path = Path("config.yaml")
    if not config_path.exists():
        return render_template_string(TEMPLATE, error="config.yaml not found next to web_app.py")

    try:
        zip_path = _run_builder_capture_zip(config_path, job_folder_name, sterile_flag)
        data = zip_path.read_bytes()
        # Attempt to cleanup the entire temp directory that contains the zip
        try:
            shutil.rmtree(zip_path.parent)
        except Exception:
            pass
    except SystemExit as e:
        return render_template_string(TEMPLATE, error=f"Build failed: {e}")
    except Exception as e:
        return render_template_string(TEMPLATE, error=f"Unexpected error: {e}")

    # Stream zip bytes directly (no persistent file) – name it the job folder
    return send_file(io.BytesIO(data), as_attachment=True, download_name=f"{job_folder_name}.zip")


if __name__ == "__main__":
    app.run(host="127.0.0.1", port=5000, debug=False)


