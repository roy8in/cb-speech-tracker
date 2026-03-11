"""
Central Bank Watchtower — Data Exporter

Exports analysis data to CSV/Excel for Tableau and other tools.
Also provides direct access to the SQLite .db file.
"""

import sys
import csv
import json
import shutil
import logging
from pathlib import Path
from datetime import datetime

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

logger = logging.getLogger(__name__)

DEFAULT_OUTPUT_DIR = Path(__file__).parent / "data" / "exports"


class DataExporter:
    """Export speech analysis data in multiple formats."""

    def __init__(self, db=None, output_dir=None):
        from .models import SpeechDB
        self.db = db or SpeechDB()
        self.output_dir = Path(output_dir) if output_dir else DEFAULT_OUTPUT_DIR
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def export_all(self):
        """Export all datasets."""
        files = []
        files.append(self.export_speeches_analysis())
        files.append(self.export_member_timeline())
        files.append(self.export_bank_stance())
        files.append(self.copy_db())
        logger.info(f"Exported {len(files)} files to {self.output_dir}")
        return files

    def export_speeches_analysis(self, filename='speeches_analysis.csv'):
        """
        Export speeches joined with analysis results.
        Columns: bank, speaker, title, date, url, hawk_dove_score, confidence,
                 topics, key_phrases, model_used
        """
        conn = self.db._get_conn()
        try:
            rows = conn.execute("""
                SELECT
                    s.bank,
                    s.speaker,
                    s.title,
                    s.date,
                    s.url,
                    s.speech_type,
                    s.language,
                    s.full_text,
                    a.hawk_dove_score,
                    a.confidence,
                    a.topics_json,
                    a.key_phrases,
                    a.summary,
                    a.model_used,
                    a.analyzed_at
                FROM speeches s
                LEFT JOIN analysis_results a ON s.id = a.speech_id
                ORDER BY s.date DESC
            """).fetchall()

            output_path = self.output_dir / filename
            with open(output_path, 'w', newline='', encoding='utf-8-sig') as f:
                writer = csv.writer(f)
                writer.writerow([
                    'bank', 'speaker', 'title', 'date', 'url', 'speech_type',
                    'language', 'full_text', 'hawk_dove_score', 'confidence', 'topics',
                    'key_phrases', 'summary', 'model_used', 'analyzed_at'
                ])
                for row in rows:
                    writer.writerow([
                        row['bank'], row['speaker'], row['title'], row['date'],
                        row['url'], row['speech_type'], row['language'], row['full_text'],
                        row['hawk_dove_score'], row['confidence'],
                        row['topics_json'], row['key_phrases'],
                        row['summary'], row['model_used'], row['analyzed_at']
                    ])

            logger.info(f"Exported {len(rows)} rows to {output_path}")
            return str(output_path)
        finally:
            conn.close()

    def export_member_timeline(self, filename='member_timeline.csv'):
        """
        Export per-member hawk/dove score timeline.
        Useful for Tableau line charts tracking individual members.
        """
        conn = self.db._get_conn()
        try:
            rows = conn.execute("""
                SELECT
                    s.bank,
                    s.speaker,
                    s.date,
                    s.title,
                    a.hawk_dove_score,
                    a.confidence,
                    a.model_used
                FROM speeches s
                JOIN analysis_results a ON s.id = a.speech_id
                WHERE s.speaker IS NOT NULL AND s.speaker != ''
                ORDER BY s.speaker, s.date
            """).fetchall()

            output_path = self.output_dir / filename
            with open(output_path, 'w', newline='', encoding='utf-8-sig') as f:
                writer = csv.writer(f)
                writer.writerow([
                    'bank', 'speaker', 'date', 'title',
                    'hawk_dove_score', 'confidence', 'model_used'
                ])
                for row in rows:
                    writer.writerow([
                        row['bank'], row['speaker'], row['date'], row['title'],
                        row['hawk_dove_score'], row['confidence'], row['model_used']
                    ])

            logger.info(f"Exported {len(rows)} rows to {output_path}")
            return str(output_path)
        finally:
            conn.close()

    def export_bank_stance(self, filename='bank_stance.csv'):
        """
        Export aggregated bank-level stance data.
        One row per bank per month with average score.
        """
        conn = self.db._get_conn()
        try:
            rows = conn.execute("""
                SELECT
                    s.bank,
                    substr(s.date, 1, 7) as month,
                    COUNT(*) as speech_count,
                    AVG(a.hawk_dove_score) as avg_score,
                    MIN(a.hawk_dove_score) as min_score,
                    MAX(a.hawk_dove_score) as max_score,
                    AVG(a.confidence) as avg_confidence
                FROM speeches s
                JOIN analysis_results a ON s.id = a.speech_id
                GROUP BY s.bank, substr(s.date, 1, 7)
                ORDER BY s.bank, month
            """).fetchall()

            output_path = self.output_dir / filename
            with open(output_path, 'w', newline='', encoding='utf-8-sig') as f:
                writer = csv.writer(f)
                writer.writerow([
                    'bank', 'month', 'speech_count',
                    'avg_score', 'min_score', 'max_score', 'avg_confidence'
                ])
                for row in rows:
                    writer.writerow([
                        row['bank'], row['month'], row['speech_count'],
                        round(row['avg_score'], 2) if row['avg_score'] else '',
                        round(row['min_score'], 2) if row['min_score'] else '',
                        round(row['max_score'], 2) if row['max_score'] else '',
                        round(row['avg_confidence'], 3) if row['avg_confidence'] else ''
                    ])

            logger.info(f"Exported {len(rows)} rows to {output_path}")
            return str(output_path)
        finally:
            conn.close()

    def copy_db(self, filename='speeches.db'):
        """
        Copy the SQLite database file to the export directory.
        This allows sharing the entire DB (e.g., uploading to a server).
        """
        from .models import get_db_path
        src = get_db_path()
        dst = self.output_dir / filename

        shutil.copy2(src, dst)
        logger.info(f"Copied DB to {dst}")
        return str(dst)


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description='Export speech analysis data')
    parser.add_argument('--output-dir', type=str, default=None,
                        help='Output directory for exports')
    parser.add_argument('--format', choices=['csv', 'db', 'all'], default='all',
                        help='Export format')
    args = parser.parse_args()

    exporter = DataExporter(output_dir=args.output_dir)

    if args.format == 'all':
        files = exporter.export_all()
    elif args.format == 'csv':
        files = [
            exporter.export_speeches_analysis(),
            exporter.export_member_timeline(),
            exporter.export_bank_stance(),
        ]
    elif args.format == 'db':
        files = [exporter.copy_db()]

    print(f"\nExported files:")
    for f in files:
        print(f"  {f}")
