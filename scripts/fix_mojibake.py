import sqlite3

conn = sqlite3.connect('src/data/speeches.db')
conn.row_factory = sqlite3.Row
cur = conn.cursor()

cur.execute("SELECT id, title, full_text FROM speeches")
rows = cur.fetchall()

# The mojibake is utf-8 bytes interpreted as latin1/cp1252.
# e.g., '—' is \xe2\x80\x94 in utf-8, but it got decoded to \u00e2\u0080\u0094.
replacements = {
    '\u00e2\u0080\u0094': '—',
    '\u00e2\u0080\u0093': '–',
    '\u00e2\u0080\u009c': '“',
    '\u00e2\u0080\u009d': '”',
    '\u00e2\u0080\u0098': '‘',
    '\u00e2\u0080\u0099': '’',
    '\u00e2\u0080\u00a6': '…',
    '\u00e2\u0080\u008b': '\u200b',
}

count = 0
for row in rows:
    title = row['title']
    full_text = row['full_text']
    
    new_title = title
    new_text = full_text
    
    if new_title:
        for b, g in replacements.items():
            new_title = new_title.replace(b, g)
            
    if new_text:
        for b, g in replacements.items():
            new_text = new_text.replace(b, g)
            
    if new_title != title or new_text != full_text:
        cur.execute("UPDATE speeches SET title = ?, full_text = ? WHERE id = ?", (new_title, new_text, row['id']))
        count += 1

conn.commit()
print(f"Updated {count} records.")

# Update FTS
cur.execute("INSERT INTO speeches_fts(speeches_fts) VALUES('rebuild')")
conn.commit()
conn.close()
