"""
Generates dashboard_data.json for the GitHub Pages dashboard.
Extracts stats, health status, and speaker lists from the SQLite DB.
"""

import json
import os
import sqlite3
from pathlib import Path
from datetime import datetime

# 프로젝트 루트 경로 설정
ROOT_DIR = Path(__file__).parent.parent
DB_PATH = ROOT_DIR / "src" / "data" / "speeches.db"
OUTPUT_PATH = ROOT_DIR / "docs" / "data.json"

def generate_data():
    if not DB_PATH.exists():
        print(f"Error: Database not found at {DB_PATH}")
        return

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    
    try:
        # 1. 은행별 통계
        bank_stats = {}
        rows = conn.execute("SELECT bank_code, COUNT(*) as count FROM speeches GROUP BY bank_code").fetchall()
        for r in rows:
            bank_stats[r['bank_code']] = r['count']
            
        # 2. 최근 수집 로그 (최근 5건)
        logs = []
        rows = conn.execute("SELECT * FROM collection_logs ORDER BY started_at DESC LIMIT 5").fetchall()
        for r in rows:
            logs.append({
                'started_at': r['started_at'],
                'finished_at': r['finished_at'],
                'status': r['status'],
                'total_new': r['total_new_speeches'],
                'error': r['error_message']
            })
            
        # 3. 화자 목록 (은행별)
        speakers = {}
        rows = conn.execute("SELECT bank_code, speaker, COUNT(*) as count FROM speeches WHERE speaker IS NOT NULL GROUP BY bank_code, speaker ORDER BY count DESC").fetchall()
        for r in rows:
            if r['bank_code'] not in speakers:
                speakers[r['bank_code']] = []
            speakers[r['bank_code']].append({
                'name': r['speaker'],
                'count': r['count']
            })
            
        # 4. 전체 요약
        total_speeches = conn.execute("SELECT COUNT(*) FROM speeches").fetchone()[0]
        
        dashboard_data = {
            'last_updated': datetime.now().isoformat(),
            'total_speeches': total_speeches,
            'bank_stats': bank_stats,
            'recent_logs': logs,
            'speakers': speakers,
            'health': logs[0]['status'] if logs else 'unknown'
        }
        
        # docs 디렉토리 생성
        OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
        
        with open(OUTPUT_PATH, 'w', encoding='utf-8') as f:
            json.dump(dashboard_data, f, indent=2, ensure_ascii=False)
            
        print(f"Successfully generated dashboard data at {OUTPUT_PATH}")
        
    finally:
        conn.close()

if __name__ == "__main__":
    generate_data()
