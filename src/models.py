"""
Central Bank Watchtower — Data Models (SQLite)

Tables:
  - speeches: 연설 원문 + 메타데이터
  - members: 중앙은행 위원 정보
  - analysis_results: Hawk/Dove NLP 분석 결과
  - minutes: 정책결정 회의록
"""

import sqlite3
import json
import os
from datetime import datetime
from pathlib import Path

DB_PATH = Path(__file__).parent / "data" / "speeches.db"


def get_db_path():
    """Return the database path, creating directories if needed."""
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    return str(DB_PATH)


class SpeechDB:
    """SQLite database manager for central bank speech data."""

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
        """Create tables if they don't exist."""
        conn = self._get_conn()
        try:
            conn.executescript("""
                CREATE TABLE IF NOT EXISTS speeches (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    bank TEXT NOT NULL,
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
                    bank TEXT NOT NULL,
                    name TEXT NOT NULL,
                    role TEXT,
                    term_start TEXT,
                    term_end TEXT,
                    is_voter INTEGER DEFAULT 1,
                    status TEXT DEFAULT 'active',
                    notes TEXT,
                    created_at TEXT DEFAULT (datetime('now')),
                    UNIQUE(bank, name)
                );

                CREATE TABLE IF NOT EXISTS analysis_results (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    speech_id INTEGER NOT NULL,
                    hawk_dove_score REAL,
                    confidence REAL,
                    topics_json TEXT,
                    key_phrases TEXT,
                    summary TEXT,
                    model_used TEXT,
                    analyzed_at TEXT DEFAULT (datetime('now')),
                    FOREIGN KEY (speech_id) REFERENCES speeches(id)
                );

                CREATE TABLE IF NOT EXISTS minutes (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    bank TEXT NOT NULL,
                    meeting_date TEXT NOT NULL,
                    release_date TEXT NOT NULL,
                    url TEXT UNIQUE NOT NULL,
                    title TEXT,
                    full_text TEXT,
                    fetched_at TEXT NOT NULL,
                    created_at TEXT DEFAULT (datetime('now'))
                );

                CREATE INDEX IF NOT EXISTS idx_speeches_bank ON speeches(bank);
                CREATE INDEX IF NOT EXISTS idx_speeches_date ON speeches(date);
                CREATE INDEX IF NOT EXISTS idx_speeches_speaker ON speeches(speaker);
                CREATE INDEX IF NOT EXISTS idx_speeches_url ON speeches(url);
                CREATE INDEX IF NOT EXISTS idx_members_bank ON members(bank);
                CREATE INDEX IF NOT EXISTS idx_analysis_speech ON analysis_results(speech_id);
                CREATE INDEX IF NOT EXISTS idx_minutes_bank ON minutes(bank);
            """)
            conn.commit()
        finally:
            conn.close()

    # ─── Speech CRUD ───

    def insert_speech(self, bank, speaker, title, date, url, full_text=None,
                      speech_type='speech', language='en'):
        """Insert a new speech. Returns id or None if duplicate."""
        conn = self._get_conn()
        try:
            cursor = conn.execute("""
                INSERT OR IGNORE INTO speeches
                (bank, speaker, title, date, url, full_text, speech_type, language, fetched_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (bank, speaker, title, date, url, full_text, speech_type, language,
                  datetime.now().isoformat()))
            conn.commit()
            return cursor.lastrowid if cursor.rowcount > 0 else None
        finally:
            conn.close()

    def update_speech_text(self, speech_id, full_text):
        """Update the full text of a speech."""
        conn = self._get_conn()
        try:
            conn.execute("UPDATE speeches SET full_text = ? WHERE id = ?",
                         (full_text, speech_id))
            conn.commit()
        finally:
            conn.close()

    def get_speech_by_url(self, url):
        """Get a speech by its URL."""
        conn = self._get_conn()
        try:
            row = conn.execute("SELECT * FROM speeches WHERE url = ?", (url,)).fetchone()
            return dict(row) if row else None
        finally:
            conn.close()

    def get_recent_speeches(self, limit=20, bank=None):
        """Get most recent speeches, optionally filtered by bank."""
        conn = self._get_conn()
        try:
            if bank:
                rows = conn.execute(
                    "SELECT * FROM speeches WHERE bank = ? ORDER BY date DESC LIMIT ?",
                    (bank, limit)).fetchall()
            else:
                rows = conn.execute(
                    "SELECT * FROM speeches ORDER BY date DESC LIMIT ?",
                    (limit,)).fetchall()
            return [dict(r) for r in rows]
        finally:
            conn.close()

    def get_speeches_without_analysis(self, limit=50):
        """Get speeches that haven't been analyzed yet."""
        conn = self._get_conn()
        try:
            rows = conn.execute("""
                SELECT s.* FROM speeches s
                LEFT JOIN analysis_results a ON s.id = a.speech_id
                WHERE a.id IS NULL AND s.full_text IS NOT NULL AND s.full_text != ''
                ORDER BY s.date DESC
                LIMIT ?
            """, (limit,)).fetchall()
            return [dict(r) for r in rows]
        finally:
            conn.close()

    def get_existing_urls(self, bank):
        """Get all URLs already in DB for a given bank."""
        conn = self._get_conn()
        try:
            rows = conn.execute("SELECT url FROM speeches WHERE bank = ?", (bank,)).fetchall()
            return {r['url'] for r in rows}
        finally:
            conn.close()

    def get_speech_count(self, bank=None):
        """Get total speech count."""
        conn = self._get_conn()
        try:
            if bank:
                row = conn.execute("SELECT COUNT(*) as cnt FROM speeches WHERE bank = ?",
                                   (bank,)).fetchone()
            else:
                row = conn.execute("SELECT COUNT(*) as cnt FROM speeches").fetchone()
            return row['cnt']
        finally:
            conn.close()

    # ─── Analysis CRUD ───

    def insert_analysis(self, speech_id, hawk_dove_score, confidence, topics,
                        key_phrases=None, summary=None, model_used='finbert'):
        """Insert analysis results for a speech."""
        conn = self._get_conn()
        try:
            topics_json = json.dumps(topics) if isinstance(topics, (dict, list)) else topics
            conn.execute("""
                INSERT INTO analysis_results
                (speech_id, hawk_dove_score, confidence, topics_json, key_phrases, summary, model_used)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (speech_id, hawk_dove_score, confidence, topics_json,
                  key_phrases, summary, model_used))
            conn.commit()
        finally:
            conn.close()

    def get_analysis_for_speech(self, speech_id):
        """Get analysis results for a specific speech."""
        conn = self._get_conn()
        try:
            row = conn.execute(
                "SELECT * FROM analysis_results WHERE speech_id = ?",
                (speech_id,)).fetchone()
            return dict(row) if row else None
        finally:
            conn.close()

    def get_bank_stance(self, bank, limit=20):
        """Get recent hawk/dove scores for a bank."""
        conn = self._get_conn()
        try:
            rows = conn.execute("""
                SELECT s.date, s.speaker, s.title, a.hawk_dove_score, a.confidence
                FROM speeches s
                JOIN analysis_results a ON s.id = a.speech_id
                WHERE s.bank = ?
                ORDER BY s.date DESC
                LIMIT ?
            """, (bank, limit)).fetchall()
            return [dict(r) for r in rows]
        finally:
            conn.close()

    def get_recent_speeches_with_analysis(self, limit=5, bank=None):
        """Get recent speeches joined with their NLP analysis results."""
        conn = self._get_conn()
        try:
            query = """
                SELECT s.id, s.bank, s.speaker, s.title, s.date, s.url,
                       a.hawk_dove_score, a.confidence, a.topics_json, a.summary
                FROM speeches s
                JOIN analysis_results a ON s.id = a.speech_id
            """
            params = []
            if bank:
                query += " WHERE s.bank = ?"
                params.append(bank)
            
            query += " ORDER BY s.date DESC LIMIT ?"
            params.append(limit)
            
            rows = conn.execute(query, tuple(params)).fetchall()
            return [dict(r) for r in rows]
        finally:
            conn.close()
        """Get hawk/dove score timeline for a specific speaker."""
        conn = self._get_conn()
        try:
            if bank:
                rows = conn.execute("""
                    SELECT s.date, s.bank, s.title, a.hawk_dove_score, a.confidence
                    FROM speeches s
                    JOIN analysis_results a ON s.id = a.speech_id
                    WHERE s.speaker LIKE ? AND s.bank = ?
                    ORDER BY s.date ASC
                """, (f"%{speaker_name}%", bank)).fetchall()
            else:
                rows = conn.execute("""
                    SELECT s.date, s.bank, s.title, a.hawk_dove_score, a.confidence
                    FROM speeches s
                    JOIN analysis_results a ON s.id = a.speech_id
                    WHERE s.speaker LIKE ?
                    ORDER BY s.date ASC
                """, (f"%{speaker_name}%",)).fetchall()
            return [dict(r) for r in rows]
        finally:
            conn.close()

    # ─── Members CRUD ───

    def insert_member(self, bank, name, role=None, term_start=None, term_end=None,
                      is_voter=True, status='active', notes=None):
        """Insert or update a member."""
        conn = self._get_conn()
        try:
            conn.execute("""
                INSERT OR REPLACE INTO members
                (bank, name, role, term_start, term_end, is_voter, status, notes)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (bank, name, role, term_start, term_end, int(is_voter), status, notes))
            conn.commit()
        finally:
            conn.close()

    def get_active_members(self, bank=None):
        """Get active members, optionally filtered by bank."""
        conn = self._get_conn()
        try:
            if bank:
                rows = conn.execute(
                    "SELECT * FROM members WHERE bank = ? AND status = 'active'",
                    (bank,)).fetchall()
            else:
                rows = conn.execute(
                    "SELECT * FROM members WHERE status = 'active'").fetchall()
            return [dict(r) for r in rows]
        finally:
            conn.close()

    def seed_members(self, members_json_path):
        """Load members from a JSON seed file."""
        with open(members_json_path, 'r', encoding='utf-8') as f:
            members = json.load(f)
        for m in members:
            self.insert_member(**m)

    # ─── Minutes CRUD ───

    def insert_minutes(self, bank, meeting_date, release_date, url, title=None, full_text=None):
        """Insert meeting minutes."""
        conn = self._get_conn()
        try:
            conn.execute("""
                INSERT OR IGNORE INTO minutes
                (bank, meeting_date, release_date, url, title, full_text, fetched_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (bank, meeting_date, release_date, url, title, full_text,
                  datetime.now().isoformat()))
            conn.commit()
        finally:
            conn.close()

    # ─── Stats ───

    def get_stats(self):
        """Get overall database statistics."""
        conn = self._get_conn()
        try:
            stats = {}
            for bank in self.BANKS:
                row = conn.execute(
                    "SELECT COUNT(*) as cnt FROM speeches WHERE bank = ?",
                    (bank,)).fetchone()
                analyzed = conn.execute("""
                    SELECT COUNT(*) as cnt FROM speeches s
                    JOIN analysis_results a ON s.id = a.speech_id
                    WHERE s.bank = ?
                """, (bank,)).fetchone()
                stats[bank] = {
                    'total_speeches': row['cnt'],
                    'analyzed': analyzed['cnt']
                }
            total = conn.execute("SELECT COUNT(*) as cnt FROM speeches").fetchone()
            stats['total'] = total['cnt']
            return stats
        finally:
            conn.close()


if __name__ == '__main__':
    db = SpeechDB()
    print(f"Database initialized at: {db.db_path}")
    print(f"Stats: {db.get_stats()}")
