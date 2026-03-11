# CB Speech Tracker (Central Bank Speech Archive)

중앙은행(FRB, ECB, BOE, BOJ, RBA)의 연설문을 자동으로 수집하고, SQLite FTS5를 이용해 초고속 전문 검색을 지원하는 아카이브 시스템입니다.

## 🚀 주요 기능

- **멀티 뱅크 수집**: 5대 주요 중앙은행(미국, 유럽, 영국, 일본, 호주)의 연설문 자동 크롤링
- **봇 차단 우회**: Playwright(Chromium)를 활용하여 RBA 등 강력한 보안이 적용된 사이트에서도 안정적으로 수집
- **고성능 검색 (FTS5)**: 수천 건의 연설문 본문을 대상으로 특정 키워드(예: AI, Inflation)를 0.1초 내에 검색
- **증분 업데이트**: 이미 수집된 URL은 건너뛰고 새로운 연설문만 스마트하게 추가
- **간편한 데이터 관리**: 단일 SQLite DB 파일(`.db`)로 데이터를 관리하며, 나중에 PostgreSQL로의 이관도 매우 용이함

## 🛠 설치 방법

```bash
# 의존성 설치
pip install -r requirements.txt

# Playwright 브라우저 설치 (RBA 수집용)
playwright install chromium
```

## 📖 사용 방법

### 1. 데이터 수집 실행
모든 중앙은행의 2025년 이후 연설문을 수집하여 DB에 저장합니다.
```bash
python3 main.py
```

### 2. 키워드 검색 (Python)
```python
from src.models import SpeechDB

db = SpeechDB()
results = db.search_speeches('AI')

for r in results:
    print(f"[{r['bank_code']}] {r['date']} | {r['title']}")
```

### 3. 데이터 내보내기 (CSV)
```bash
python3 src/exporter.py --format csv
```

## 📂 데이터베이스 구조 (SQLite)

- **`speeches`**: 원본 데이터 (은행 코드, 화자, 제목, 날짜, URL, 본문 전문)
- **`speeches_fts`**: 전문 검색(Full-Text Search) 전용 가상 테이블
- **`members`**: 중앙은행 위원 정보

## ⚖️ 라이선스
이 프로젝트는 교육 및 연구 목적으로 제작되었습니다. 수집된 데이터의 저작권은 각 중앙은행에 있습니다.
