# TODO: Implement Motu/OPeNDAP fetch for CMEMS Med Physics/Waves
from pathlib import Path
from src.util.logging_setup import setup_logger
logger = setup_logger()

def main():
    Path("data/raw").mkdir(parents=True, exist_ok=True)
    logger.info("Stub CMEMS fetch complete (replace with Motu client).")

if __name__ == "__main__":
    main()
