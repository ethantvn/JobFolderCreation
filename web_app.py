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
<html lang=\"en\">
<head>
  <meta charset=\"utf-8\" />
  <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\" />
  <title>CMD Job Builder</title>
  <style>
    :root {
      /* Light theme */
      --bg: #f5f7fb;        /* page background */
      --panel: #ffffff;     /* card top */
      --panel2: #f9fbff;    /* card bottom */
      --text: #0c1220;      /* primary text */
      --muted: #5a637a;     /* labels/help text */
      --brand: #3b82f6;     /* primary accent */
      --brand2: #2563eb;    /* gradient start */
      --ok: #16a34a;        /* progress fill */
      --shadow: 0 10px 28px rgba(20,31,56,.12);
    }
    * { box-sizing: border-box; }
    body { margin: 0; min-height: 100vh; display: flex; align-items: center; justify-content: center;
      font-family: ui-sans-serif, system-ui, "Segoe UI", Roboto, Arial; color: var(--text);
      background: linear-gradient(180deg, #eef2f9 0%, var(--bg) 100%);
      padding: 28px; }
    .shell { width: 100%; max-width: 900px; }
    .header { display: flex; align-items: center; justify-content: space-between; margin-bottom: 16px; }
    .brand { font-weight: 800; letter-spacing: .3px; font-size: 20px; color: var(--brand2); }
    .card { background: linear-gradient(180deg, var(--panel) 0%, var(--panel2) 100%);
      border: 1px solid #e3e8f3; border-radius: 14px; box-shadow: var(--shadow); padding: 22px; }
    .grid { display: grid; grid-template-columns: 1fr 160px; gap: 14px; }
    label { display: block; font-size: 13px; color: var(--muted); margin-bottom: 6px; }
    input[type=text]{ width: 100%; padding: 12px 14px; border-radius: 10px; border: 1px solid #d6dce8;
      background: #ffffff; color: var(--text); }
    .toggle { display: inline-flex; align-items: center; gap: 10px; user-select: none; }
    .btn { padding: 12px 16px; border-radius: 10px; border: 1px solid #cfe0ff; cursor: pointer; color: #fff;
      background: linear-gradient(90deg, var(--brand2), var(--brand)); font-weight: 700; box-shadow: 0 6px 14px rgba(37,99,235,.18); }
    .btn:disabled { opacity: .6; cursor: not-allowed; }
    .sub { margin-top: 12px; color: var(--muted); font-size: 13px; }
    .error { margin-top: 12px; color: #8b0000; background: #ffecec; border: 1px solid #ffbcbc; padding: 10px 12px; border-radius: 10px; }
    #bar { height: 14px; background: #e9eef8; margin-top: 16px; border-radius: 999px; overflow: hidden;
      border: 1px solid #d6dce8; display: none; }
    #fill { height: 100%; width: 0; background: linear-gradient(90deg, #22c55e, var(--ok)); transition: width .25s ease; }
  </style>
  <script>
    async function onSubmitForm(e){
      e.preventDefault();
      const btn = document.getElementById('runBtn');
      const jf = document.getElementById('job_name');
      const sterile = document.getElementById('sterile').checked;
      const errBox = document.getElementById('err');
      errBox.textContent = '';
      errBox.style.display = 'none';
      btn.disabled = true; btn.innerText = 'Starting…';
      document.getElementById('bar').style.display = 'block';

      const res = await fetch('/start', {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ job_folder_name: jf.value, sterile: sterile })
      });
      if (!res.ok) { btn.disabled = false; btn.innerText = 'Build and Download ZIP'; return; }
      const { job_id } = await res.json();
      btn.innerText = 'Building… 0%';
      await pollProgress(job_id, jf.value, btn);
    }

    async function pollProgress(jobId, jobName, btn){
      try {
        const r = await fetch('/progress?job_id=' + encodeURIComponent(jobId));
        const j = await r.json();
        if (j.error) {
          const errBox = document.getElementById('err');
          errBox.textContent = j.error;
          errBox.style.display = 'block';
          document.getElementById('bar').style.display = 'none';
          btn.disabled = false; btn.innerText = 'Build and Download ZIP';
          return;
        }
        const pct = Math.max(0, Math.min(100, j.percent|0));
        document.getElementById('fill').style.width = pct + '%';
        btn.innerText = 'Building… ' + pct + '%';
        if (pct < 100) setTimeout(()=>pollProgress(jobId, jobName, btn), 500);
        else {
          btn.innerText = 'Downloading…';
          window.location = '/download?job_id=' + encodeURIComponent(jobId) + '&name=' + encodeURIComponent(jobName);
          setTimeout(()=>{ btn.disabled = false; btn.innerText = 'Build and Download ZIP'; }, 1500);
        }
      } catch(e){ setTimeout(()=>pollProgress(jobId, jobName, btn), 800); }
    }
  </script>
  </head>
  <body>
    <div class=\"shell\">
      <div class=\"header\"><div class=\"brand\">CMD Job Builder</div></div>
      <div class=\"card\">
        <form method=\"POST\" onsubmit=\"return onSubmitForm(event)\">
          <div class=\"grid\">
            <div>
              <label>Job Folder Name</label>
              <input id=\"job_name\" type=\"text\" name=\"job_folder_name\" placeholder=\"J-25-B124 (VSR)\" required />
            </div>
            <div>
              <label>&nbsp;</label>
              <label class=\"toggle\"><input id=\"sterile\" type=\"checkbox\" name=\"sterile\" checked /> Sterile</label>
            </div>
          </div>
          <div style=\"display:flex; gap:12px; align-items:center; margin-top:14px;\">
            <button id=\"runBtn\" class=\"btn\">Build and Download ZIP</button>
          </div>
          <div class=\"sub\">Uses config.yaml for paths; overrides job folder name and sterile.</div>
          <div id="err" class="error" style="display:none"></div>
          {% if error %}<div class=\"error\">{{ error }}</div>{% endif %}
          <div id=\"bar\"><div id=\"fill\"></div></div>
        </form>
      </div>
    </div>
  </body>
 </html>
"""


progress_store = {}


def _run_builder_capture_zip(config_path: Path, job_folder_name: str, sterile: bool, job_id: Optional[str] = None) -> Path:
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
        # Skip copying if an AM Tracker already exists anywhere in the temp job root
        existing = False
        for p in tmp_job.rglob("*.xlsx"):
            if "am tracker" in p.name.lower():
                existing = True
                break
        if not existing:
            for p in tmp_job.rglob("*.xlsm"):
                if "am tracker" in p.name.lower():
                    existing = True
                    break

        if not existing and orig_docs.is_dir():
            # Collect candidates from Docs and choose the newest one
            candidates = [
                f for f in orig_docs.iterdir()
                if f.is_file() and f.suffix.lower() in (".xlsx", ".xlsm") and "am tracker" in f.name.lower()
            ]
            if candidates:
                best = max(candidates, key=lambda x: x.stat().st_mtime)
                tmp_docs.mkdir(parents=True, exist_ok=True)
                shutil.copy2(best, tmp_docs / best.name)
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
    # Progress reporting
    jid = job_id or f"job_{os.getpid()}_{threading.get_ident()}"
    def cb(pct: int):
        progress_store[jid] = max(0, min(100, int(pct)))

    run_builder(cfg, verbose=True, dry_run=False, progress_callback=cb)

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


@app.route("/", methods=["GET"])
def index():
    return render_template_string(TEMPLATE)


@app.route("/start", methods=["POST"])
def start():
    body = request.get_json(force=True, silent=True) or {}
    job_folder_name = str(body.get("job_folder_name", "")).strip()
    sterile_flag = bool(body.get("sterile", False))
    if not job_folder_name:
        return {"error": "Job folder name is required"}, 400

    config_path = Path("config.yaml")
    if not config_path.exists():
        return {"error": "config.yaml not found"}, 400

    job_id = f"job_{os.getpid()}_{threading.get_ident()}"
    progress_store[job_id] = 0

    def runner():
        try:
            zip_path = _run_builder_capture_zip(config_path, job_folder_name, sterile_flag, job_id)
            progress_store[job_id] = 100
            progress_store[job_id + ":zip"] = str(zip_path)
        except BaseException as e:
            progress_store[job_id] = -1
            progress_store[job_id + ":err"] = str(e)

    t = threading.Thread(target=runner, daemon=True)
    t.start()
    return {"job_id": job_id}


@app.route("/download", methods=["GET"])
def download():
    job_id = request.args.get("job_id", "")
    job_folder_name = request.args.get("name", "job_build").strip() or "job_build"
    err = progress_store.get(job_id + ":err")
    if err:
        return render_template_string(TEMPLATE, error=f"Build failed: {err}")
    zip_path_str = progress_store.get(job_id + ":zip")
    if not zip_path_str:
        return render_template_string(TEMPLATE, error="ZIP not ready yet")
    zip_path = Path(zip_path_str)
    if not zip_path.exists():
        return render_template_string(TEMPLATE, error="ZIP not found on disk")
    data = zip_path.read_bytes()
    try:
        shutil.rmtree(zip_path.parent)
    except Exception:
        pass
    return send_file(io.BytesIO(data), as_attachment=True, download_name=f"{job_folder_name}.zip")


@app.route("/progress", methods=["GET"])
def progress():
    jid = request.args.get("job_id", "")
    if not jid or jid not in progress_store:
        return {"error": "Job not found. Check job name and spacing."}
    err = progress_store.get(jid + ":err")
    if err:
        return {"error": "Job not found. Check job name and spacing."}
    pct = int(progress_store.get(jid, 0))
    return {"percent": pct}


if __name__ == "__main__":
    app.run(host="127.0.0.1", port=5000, debug=False)


