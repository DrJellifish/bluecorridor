# TODO: Implement ECMWF CDS or GFS NOMADS fetch for 10 m winds
from pathlib import Path
from src.util.logging_setup import setup_logger
logger = setup_logger()

def main():
    Path("data/raw").mkdir(parents=True, exist_ok=True)
    logger.info("Stub wind fetch complete (replace with ECMWF/GFS).")

if __name__ == "__main__":
    main()
