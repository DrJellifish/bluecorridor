from pathlib import Path
from src.util.logging_setup import setup_logger
logger = setup_logger()

def main():
    Path("site/data/latest").mkdir(parents=True, exist_ok=True)
    # Copy latest artifacts later (wired in Makefile publish step)
    logger.info("GeoJSON publish stub.")

if __name__ == "__main__":
    main()
