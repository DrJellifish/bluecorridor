# Blue Corridor

**bluecorridor** is a Mediterranean surface drift forecasting pipeline for aid delivery — combining forecast fields with particle tracking to identify optimal release points and times.

## Quickstart
1. Create environment:
   ```bash
   conda env create -f env/environment.yml && conda activate bluecorridor
   ```
2. Configure:
   ```bash
   cp .env.example .env
   # edit credentials
   ```
3. Run the full pipeline:
   ```bash
   make -C ops all
   ```

See `RUNBOOK.md` for daily operations.
