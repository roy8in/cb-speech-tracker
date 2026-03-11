"""
Central Bank Watchtower — Data Models (SQLite Optimized)

Tables:
  - speeches: 원본 연설 데이터 및 메타데이터
  - speeches_fts: 전문 검색(Full-Text Search)을 위한 가상 테이블
  - members: 중앙은행 위원 정보
"""

import sqlite3
import os
from datetime import datetime
from pathlib import Path

DB_PATH = Path(__file__).parent / "data" / "speeches.db"

def get_db_path():
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    return str(DB_PATH)

class SpeechDB:
    BANKS = ('FRB', 'ECB', 'BOE', 'BOJ', 'RBA', 'BOC')

    def __init__(self, db_path=None):
        self.db_path = db_path or get_db_path()
        self._init_db()

    def _get_conn(self):
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA foreign_keys=ON")
        return conn

    def _init_db(self):
        """데이터베이스 초기화 및 인덱스 설정"""
        conn = self._get_conn()
        try:
            # 1. 메인 연설 테이블
            conn.executescript("""
                CREATE TABLE IF NOT EXISTS speeches (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    bank_code TEXT NOT NULL,
                    speaker TEXT,
                    title TEXT NOT NULL,
                    date TEXT NOT NULL,
                    url TEXT UNIQUE NOT NULL,
                    full_text TEXT,
                    speech_type TEXT DEFAULT 'speech',
                    language TEXT DEFAULT 'en',
                    fetched_at TEXT NOT NULL,
                    created_at TEXT DEFAULT (datetime('now'))
                );

                CREATE TABLE IF NOT EXISTS members (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    bank_code TEXT NOT NULL,
                    name TEXT NOT NULL,
                    role TEXT,
                    status TEXT DEFAULT 'active',
                    UNIQUE(bank_code, name)
                );

                CREATE INDEX IF NOT EXISTS idx_speeches_bank ON speeches(bank_code);
                CREATE INDEX IF NOT EXISTS idx_speeches_date ON speeches(date);
                CREATE INDEX IF NOT EXISTS idx_speeches_speaker ON speeches(speaker);
            """)
            
            # 2. FTS5 전문 검색 테이블 (SQLite FTS5 모듈 필요)
            try:
                conn.execute("CREATE VIRTUAL TABLE IF NOT EXISTS speeches_fts USING fts5(title, full_text, content='speeches', content_rowid='id')")
                # 트리거 생성: 원본 테이블에 데이터 삽입/수정/삭제 시 FTS 테이블 자동 업데이트
                conn.executescript("""
                    CREATE TRIGGER IF NOT EXISTS speeches_ai AFTER INSERT ON speeches BEGIN
                      INSERT INTO speeches_fts(rowid, title, full_text) VALUES (new.id, new.title, new.full_text);
                    END;
                    CREATE TRIGGER IF NOT EXISTS speeches_ad AFTER DELETE ON speeches BEGIN
                      INSERT INTO speeches_fts(speeches_fts, rowid, title, full_text) VALUES('delete', old.id, old.title, old.full_text);
                    END;
                    CREATE TRIGGER IF NOT EXISTS speeches_au AFTER UPDATE ON speeches BEGIN
                      INSERT INTO speeches_fts(speeches_fts, rowid, title, full_text) VALUES('delete', old.id, old.title, old.full_text);
                      INSERT INTO speeches_fts(rowid, title, full_text) VALUES (new.id, new.title, new.full_text);
                    END;
                """)
            except sqlite3.OperationalError:
                # FTS5가 지원되지 않는 환경일 경우 건너뜀
                pass
                
            conn.commit()
        finally:
            conn.close()

    def insert_speech(self, bank_code, speaker, title, date, url, full_text=None, speech_type='speech', language='en'):
        """새 연설 삽입 (URL 중복 시 무시)"""
        conn = self._get_conn()
        try:
            cursor = conn.execute("""
                INSERT OR IGNORE INTO speeches 
                (bank_code, speaker, title, date, url, full_text, speech_type, language, fetched_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (bank_code, speaker, title, date, url, full_text, speech_type, language, datetime.now().isoformat()))
            conn.commit()
            return cursor.lastrowid if cursor.rowcount > 0 else None
        finally:
            conn.close()

    def get_existing_urls(self, bank_code):
        """특정 은행의 이미 수집된 URL 목록 조회"""
        conn = self._get_conn()
        try:
            rows = conn.execute("SELECT url FROM speeches WHERE bank_code = ?", (bank_code,)).fetchall()
            return {r['url'] for r in rows}
        finally:
            conn.close()

    def search_speeches(self, keyword):
        """FTS5를 이용한 초고속 키워드 검색"""
        conn = self._get_conn()
        try:
            rows = conn.execute("""
                SELECT s.bank_code, s.date, s.speaker, s.title 
                FROM speeches s
                JOIN speeches_fts f ON s.id = f.rowid
                WHERE speeches_fts MATCH ?
                ORDER BY rank
            """, (keyword,)).fetchall()
            return [dict(r) for r in rows]
        finally:
            conn.close()

    def get_stats(self):
        conn = self._get_conn()
        try:
            stats = {}
            rows = conn.execute("SELECT bank_code, COUNT(*) as cnt FROM speeches GROUP BY bank_code").fetchall()
            for r in rows:
                stats[r['bank_code']] = r['cnt']
            total = conn.execute("SELECT COUNT(*) as cnt FROM speeches").fetchone()
            stats['total'] = total['cnt']
            return stats
        finally:
            conn.close()

if __name__ == '__main__':
    db = SpeechDB()
    print(f"Database initialized. Total speeches: {db.get_stats().get('total', 0)}")
