"""
Generates dashboard_data.json for the GitHub Pages dashboard.
Extracts stats, health status, and recent speeches from the SQLite DB.
"""

import json
import sqlite3
from pathlib import Path
from datetime import datetime, timedelta

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
        banks = ['FRB', 'ECB', 'BOE', 'BOJ', 'RBA', 'BOC']
        bank_stats = {bank: 0 for bank in banks}
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
            
        # 3. 최근 연설 (넉넉하게 최근 14일치 탐색)
        # 오늘이 3월 12일이면 3월 5일 이후 데이터를 가져옵니다.
        threshold_date = (datetime.now() - timedelta(days=14)).strftime('%Y-%m-%d')
        recent_speeches = []
        rows = conn.execute("""
            SELECT bank_code, speaker, title, date, url 
            FROM speeches 
            WHERE date >= ? 
            ORDER BY date DESC, fetched_at DESC
            LIMIT 50
        """, (threshold_date,)).fetchall()
        
        for r in rows:
            recent_speeches.append({
                'bank': r['bank_code'],
                'speaker': r['speaker'],
                'title': r['title'],
                'date': r['date'],
                'url': r['url']
            })
            
        # 4. 전체 요약
        total_speeches = conn.execute("SELECT COUNT(*) FROM speeches").fetchone()[0]
        
        dashboard_data = {
            'last_updated': datetime.now().isoformat(),
            'total_speeches': total_speeches,
            'bank_stats': bank_stats,
            'recent_logs': logs,
            'recent_speeches': recent_speeches,
            'health': logs[0]['status'] if logs else 'unknown'
        }
        
        # docs 디렉토리 생성
        OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
        
        with open(OUTPUT_PATH, 'w', encoding='utf-8') as f:
            json.dump(dashboard_data, f, indent=2, ensure_ascii=False)
            
        print(f"Successfully generated dashboard data at {OUTPUT_PATH}. Found {len(recent_speeches)} recent speeches.")
        
    finally:
        conn.close()

if __name__ == "__main__":
    generate_data()
