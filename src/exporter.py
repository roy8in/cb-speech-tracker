"""
Central Bank Watchtower — Data Exporter

Exports collected speech data to CSV for external use.
Also provides direct access to the SQLite .db file.
"""

import sys
import csv
import shutil
import logging
from pathlib import Path
from datetime import datetime

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

logger = logging.getLogger(__name__)

DEFAULT_OUTPUT_DIR = Path(__file__).parent / "data" / "exports"


class DataExporter:
    """Export speech data in multiple formats."""

    def __init__(self, db=None, output_dir=None):
        from .models import SpeechDB
        self.db = db or SpeechDB()
        self.output_dir = Path(output_dir) if output_dir else DEFAULT_OUTPUT_DIR
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def export_all(self):
        """Export all datasets."""
        files = []
        files.append(self.export_speeches())
        files.append(self.copy_db())
        logger.info(f"Exported {len(files)} files to {self.output_dir}")
        return files

    def export_speeches(self, filename='speeches.csv'):
        """
        Export all speeches.
        Columns: bank, speaker, title, date, url, speech_type, language, full_text
        """
        conn = self.db._get_conn()
        try:
            rows = conn.execute("""
                SELECT
                    s.bank_code,
                    m.name as speaker,
                    s.title,
                    s.date,
                    s.url,
                    s.speech_type,
                    s.language,
                    s.full_text,
                    s.fetched_at
                FROM speeches s
                LEFT JOIN members m ON s.speaker_id = m.id
                ORDER BY s.date DESC
            """).fetchall()

            output_path = self.output_dir / filename
            with open(output_path, 'w', newline='', encoding='utf-8-sig') as f:
                writer = csv.writer(f)
                writer.writerow([
                    'bank', 'speaker', 'title', 'date', 'url', 'speech_type',
                    'language', 'full_text', 'fetched_at'
                ])
                for row in rows:
                    writer.writerow([
                        row['bank_code'], row['speaker'], row['title'], row['date'],
                        row['url'], row['speech_type'], row['language'],
                        row['full_text'], row['fetched_at']
                    ])

            logger.info(f"Exported {len(rows)} rows to {output_path}")
            return str(output_path)
        finally:
            conn.close()

    def copy_db(self, filename='speeches.db'):
        """
        Copy the SQLite database file to the export directory.
        """
        from .models import get_db_path
        src = get_db_path()
        dst = self.output_dir / filename

        shutil.copy2(src, dst)
        logger.info(f"Copied DB to {dst}")
        return str(dst)


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description='Export speech data')
    parser.add_argument('--output-dir', type=str, default=None,
                        help='Output directory for exports')
    parser.add_argument('--format', choices=['csv', 'db', 'all'], default='all',
                        help='Export format')
    args = parser.parse_args()

    exporter = DataExporter(output_dir=args.output_dir)

    if args.format == 'all':
        files = exporter.export_all()
    elif args.format == 'csv':
        files = [exporter.export_speeches()]
    elif args.format == 'db':
        files = [exporter.copy_db()]

    print(f"\nExported files:")
    for f in files:
        print(f"  {f}")
