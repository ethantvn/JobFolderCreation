## CMD Job Folder Builder

Build consistent, validated job folders for CMD work. Point the tool at your run data and it will:
- Find each PO folder
- Create `_VSR MBR` and `_VSR S&C` siblings
- Copy source files into S&C and verify counts
- Select templates from `MBR Template Map.xlsx`
- Generate CoC, Final QC, and Dimension files
- Write `item_audit.csv`
- Check for placeholders and zip the job folder

### Prerequisites
- Python 3.9+
- `pip` available (see `requirements.txt`)

### Install
```bash
pip install -r requirements.txt
```

### Configure
Edit `config.yaml` (if you don’t have one, copy `config.example.yaml` to `config.yaml` and adjust).

Key settings:
- `run_data_root`: Parent folder that contains your job folder
- `job_folder_name`: The job folder name (the ZIP will use this)
- `cmd_rel_path`: Where the CMD source PO folders live inside the job folder
- `templates_map_path` and `fake_825_root`: Where templates are found
- `sterile`: Chooses the correct CoC template

Example:
```yaml
job_number: "J-25-XXXX"
job_folder_name: "J-25-XXXX (VSR)"
sterile: true
run_data_root: 'P:\\2025 Run Data'
templates_map_path: './maps/MBR Template Map.xlsx'
fake_825_root: 'Q:\\825 - Monitoring & Measurement'
dropbox_root: ''
cmd_rel_path: 'docs\\CMD'
reset_sc_on_rerun: true
```

### Prepare your source folders
Inside `<run_data_root>/<job_folder_name>/<cmd_rel_path>/`, include only source PO folders (directory names containing `PO####`). Do not include generic `MBR` or `S&C` folders there.

### Run (CLI)
```bash
python cmd_job_builder.py --config config.yaml --verbose
# Dry run (no writes):
python cmd_job_builder.py --config config.yaml --verbose --dry-run
```

### Optional: Web UI
Run a small local site to enter `job_folder_name` and download the ZIP.
```bash
pip install -r requirements.txt
python web_app.py
# then open http://localhost:5000
```

### What you’ll get
For each source PO directory under `<cmd_rel_path>` the tool creates, side-by-side:
- `PO####_<Lot>_VSR MBR`
- `PO####_<Lot>_VSR S&C`

MBR outputs include:
- `PO####_<Lot>_CoC.docx`
- `PO####_<Lot>_<Prefix>_Final_QC.xlsx` (if mapping exists)
- `PO####_<Lot>_<Prefix>_Dimension.xlsx` (if mapping exists)
- `item_audit.csv`

Finally, it zips the job folder as `<job_folder_name>.zip` in the parent of the job folder.

### Template Map expectations
Your workbook should include rows like:
- “Item starts with C use:” with column B = Final QC template and column C = Dimension template
- “CofC for Sterile” and “CofC for Non-Sterile” rows mapping to `.docx`

All referenced templates must exist under `fake_825_root`.

### Notes
- PDF parsing uses text extraction (no layout) with tolerant regexes
- Excludes item codes matching `^aprevo\d+$`
- Numeric-starting items are included in CoC, excluded from QC/Dim
