## CMD Job Folder Builder (No-AI)

Deterministic Python CLI that reproduces the “perfect job folder” workflow for CMD jobs per WI steps 5.0–5.11.

### Features
- Validates CMD job folder structure and discovers source PO folders
- Extracts PO number, lot number, and parses item tables from PDFs
- Resolves versions from Primary/Derived Part Versions sections (fallback to table)
- Creates flat sibling `_VSR MBR` and `_VSR S&C` directories for each PO
- Recursively copies source into `_VSR S&C` and verifies file/dir counts
- Selects templates from `MBR Template Map.xlsx`
- Generates CoC `.docx` and per-prefix Final QC/Dimension `.xlsx/.xlsm`
- Writes `item_audit.csv` per PO
- Validates there are no leftover placeholders and final zips the job folder
- Optional `--dry-run` + `run_report.txt`

### Requirements
- Python 3.9+
- See `requirements.txt`

### Install
```bash
pip install -r requirements.txt
```

### Configure
Copy `config.example.yaml` to `config.yaml` and edit paths and flags.

```yaml
job_number: "J-25-XXXX"
job_folder_name: "J-25-XXXX (VSR)"
sterile: true
run_data_root: "C:/.../S3D MFG PREP/2025 Run Data"
templates_map_path: "C:/.../MBR Template Map.xlsx"
fake_825_root: "C:/.../Empty Templates/Fake 825"
dropbox_root: ""
cmd_rel_path: "CMD"
reset_sc_on_rerun: true
```

### Run
```bash
python cmd_job_builder.py --config config.yaml --verbose
# or dry-run
python cmd_job_builder.py --config config.yaml --verbose --dry-run
```

### Web UI (optional)
Run a simple local website to enter `job_folder_name` and download the ZIP.

```bash
pip install -r requirements.txt
python web_app.py
# open http://localhost:5000
```


### Expected CMD structure
`<run_data_root>/<job_folder_name>/CMD/` must contain only source PO folders (any dir whose name contains `PO####`). Generic `CMD/MBR` or `CMD/S&C` folders are forbidden.

### Output
For each source PO directory:
- Creates sibling pair under `CMD/`:
  - `PO####_<Lot>_VSR MBR`
  - `PO####_<Lot>_VSR S&C`
- MBR outputs:
  - `PO####_<Lot>_CoC.docx`
  - `PO####_<Lot>_<Prefix>_Final_QC.xlsx` (if mapping exists)
  - `PO####_<Lot>_<Prefix>_Dimension.xlsx` (if mapping exists)
  - `item_audit.csv`

Finally, zips the job folder into `<job_folder_name>.zip` in the parent directory.

### Template Map
The workbook should contain rows like:
- `Item starts with C use:` … with column B = Final QC template, column C = Dim template
- `CofC for Sterile` and `CofC for Non-Sterile` rows mapping to `.docx`

All referenced templates must exist under `fake_825_root`.

### Notes
- PDF parsing uses text extraction (no layout), tolerant regexes
- Excludes item codes matching `^aprevo\d+$`
- Numeric-starting items are included in CoC, excluded from QC/Dim


