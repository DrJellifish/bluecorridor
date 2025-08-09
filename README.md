# Blue Corridor

**bluecorridor** is a Mediterranean surface drift forecasting pipeline for aid delivery — combining forecast fields with particle tracking to identify optimal release points and times.

# Blue Corridor

**bluecorridor** is a Mediterranean surface drift forecasting pipeline for aid delivery — combining forecast fields with particle tracking to identify optimal release points and times.

---

## Quickstart — No Conda Required

1. **Create and activate a virtual environment**
   ```bash
   python3 -m venv .venv
   source .venv/bin/activate


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
