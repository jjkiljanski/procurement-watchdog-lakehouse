# Gold Report

Report file:

- `examples/gold_v1_report.qmd`

Default Gold input path inside the report:

- `E:\git_projects\procurement-watchdog-api-exploration\data\gold`

You can override it with:

```powershell
$env:GOLD_ROOT = "E:\git_projects\procurement-watchdog-api-exploration\data\gold"
```

Render:

```powershell
quarto render examples/gold_v1_report.qmd
```

If rendering fails with missing Jupyter modules (`yaml`, `notebook`, etc.), install:

```powershell
py -m pip install jupyter pyyaml
```
