import sqlite3
import os
import sys

DB_PATH = 'src/data/speeches.db'

def fix_mojibake(text):
    if not text:
        return text
    # Known mojibake patterns where utf-8 was decoded as cp949/euc-kr.
    # \xe2\x80\x94 -> em dash, \xe2\x80\x9c -> left double quote, etc.
    replacements = {
        '창\x80\x94': '—',
        '창\x80\x93': '–',
        '창\x80\x9c': '“',
        '창\x80\x9d': '”',
        '창\x80\x98': '‘',
        '창\x80\x99': '’',
        '창\x80\xa6': '…',
        '창\x80\x8b': '\u200b', # zero width space? Actually \xe2\x80\x8b.
    }
    for bad, good in replacements.items():
        text = text.replace(bad, good)
    return text

def main():
    if not os.path.exists(DB_PATH):
        print("Database not found.")
        sys.exit(1)
        
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    
    print("Populating members table...")
    cur.execute("SELECT DISTINCT bank_code, speaker FROM speeches WHERE speaker IS NOT NULL")
    speakers = cur.fetchall()
    
    member_map = {} # (bank_code, speaker_name) -> member_id
    for row in speakers:
        bank_code = row['bank_code']
        name = row['speaker']
        cur.execute("SELECT id FROM members WHERE bank_code = ? AND name = ?", (bank_code, name))
        member = cur.fetchone()
        if member:
            member_id = member['id']
        else:
            cur.execute("INSERT INTO members (bank_code, name) VALUES (?, ?)", (bank_code, name))
            member_id = cur.lastrowid
        member_map[(bank_code, name)] = member_id
        
    print(f"Mapped {len(member_map)} members.")
    
    print("Creating new speeches table...")
    cur.execute("""
        CREATE TABLE IF NOT EXISTS speeches_new (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            bank_code TEXT NOT NULL,
            speaker_id INTEGER,
            title TEXT NOT NULL,
            date TEXT NOT NULL,
            url TEXT UNIQUE NOT NULL,
            full_text TEXT,
            speech_type TEXT DEFAULT 'speech',
            language TEXT DEFAULT 'en',
            fetched_at TEXT NOT NULL,
            created_at TEXT DEFAULT (datetime('now')),
            FOREIGN KEY (speaker_id) REFERENCES members (id)
        )
    """)
    
    print("Migrating and cleaning data...")
    cur.execute("SELECT * FROM speeches")
    speeches = cur.fetchall()
    
    insert_sql = """
        INSERT INTO speeches_new (id, bank_code, speaker_id, title, date, url, full_text, speech_type, language, fetched_at, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    
    for row in speeches:
        speaker_name = row['speaker']
        speaker_id = member_map.get((row['bank_code'], speaker_name)) if speaker_name else None
        
        fixed_title = fix_mojibake(row['title'])
        fixed_text = fix_mojibake(row['full_text'])
        
        cur.execute(insert_sql, (
            row['id'],
            row['bank_code'],
            speaker_id,
            fixed_title,
            row['date'],
            row['url'],
            fixed_text,
            row['speech_type'],
            row['language'],
            row['fetched_at'],
            row['created_at']
        ))
        
    print("Swapping tables...")
    cur.execute("DROP TABLE speeches")
    cur.execute("ALTER TABLE speeches_new RENAME TO speeches")
    
    print("Recreating indexes...")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_speeches_bank ON speeches(bank_code)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_speeches_date ON speeches(date)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_speeches_speaker ON speeches(speaker_id)")
    
    # Recreate FTS
    print("Rebuilding FTS table...")
    cur.execute("DROP TABLE IF EXISTS speeches_fts")
    cur.execute("CREATE VIRTUAL TABLE IF NOT EXISTS speeches_fts USING fts5(title, full_text, content='speeches', content_rowid='id')")
    cur.execute("INSERT INTO speeches_fts(speeches_fts) VALUES('rebuild')")
    
    print("Recreating triggers...")
    cur.execute("DROP TRIGGER IF EXISTS speeches_ai")
    cur.execute("DROP TRIGGER IF EXISTS speeches_ad")
    cur.execute("DROP TRIGGER IF EXISTS speeches_au")
    
    cur.executescript("""
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
    
    conn.commit()
    conn.close()
    print("Cleanup and migration completed successfully.")

if __name__ == '__main__':
    main()
