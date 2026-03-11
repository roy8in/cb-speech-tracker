import os
import sys
from pathlib import Path

# Add src to python path so it can resolve src/cb_speeches
sys.path.append(str(Path(__file__).parent))

from src.models import SpeechDB
from src.analyzer import HawkDoveAnalyzer

def run_pipeline():
    print("Starting Central Bank Speech Pipeline...")
    # NOTE: Scraper trigger logic goes here.
    # We will initialize DB and analyzer merely to check if imports work.
    try:
        db = SpeechDB()
        analyzer = HawkDoveAnalyzer(db=db)
        print("Successfully connected to Database and mounted Analyzer.")
    except Exception as e:
        print(f"Error during initialization: {e}")

if __name__ == "__main__":
    run_pipeline()
