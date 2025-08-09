# TODO: Wire to OpenDrift/Parcels with real drivers
from pathlib import Path
import json, datetime as dt
from src.util.logging_setup import setup_logger
from src.model.particle_params import load_particle_params

logger = setup_logger()

def main():
    params = load_particle_params()
    Path("data/outputs").mkdir(parents=True, exist_ok=True)
    manifest = {
        "run_time": dt.datetime.utcnow().isoformat() + "Z",
        "params": params,
        "artifacts": {
            "tracks_geojson": "data/outputs/tracks_latest.geojson",
            "summary_csv": "data/outputs/summary_latest.csv"
        }
    }
    with open("data/outputs/tracks_latest.geojson", "w") as f:
        f.write('{"type":"FeatureCollection","features":[]}')
    with open("data/outputs/summary_latest.csv", "w") as f:
        f.write("metric,value\np50_eta_hours,NaN\n")
    with open("data/outputs/manifest.json", "w") as f:
        json.dump(manifest, f, indent=2)
    logger.info("Stub particle run complete. Artifacts written.")

if __name__ == "__main__":
    main()
