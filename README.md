# CB Speech Tracker (Central Bank Speech Archive)

주요 중앙은행(FRB, ECB, BOE, BOJ, RBA, BOC)의 연설문을 자동으로 수집하고, SQLite FTS5를 이용해 초고속 전문 검색을 지원하는 아카이브 시스템입니다.

## 🚀 주요 기능

- **자동 수집 파이프라인**: GitHub Actions를 통해 매일 최신 연설문을 자동으로 수집합니다.
- **다양한 중앙은행 지원**: FRB(미국), ECB(유럽), BOE(영국), BOJ(일본), RBA(호주), BOC(캐나다)의 연설문을 추적합니다.
- **관계형 데이터 구조**: 연설자(Members)와 연설문(Speeches)을 분리하여 데이터 무결성을 유지하고 효율적인 관리가 가능합니다.
- **모니터링 대시보드**: 수집 현황, 시스템 상태, 최근 연설 내역을 웹(GitHub Pages)에서 확인할 수 있습니다.
- **고성능 검색 (FTS5)**: 수천 건의 연설문 본문을 대상으로 특정 키워드(예: AI, Inflation)를 0.1초 내에 검색합니다.
- **데이터 익스포트**: 수집된 데이터를 CSV 및 SQLite DB 포맷으로 간편하게 추출할 수 있습니다.

## 📊 모니터링 대시보드

수집 상태와 통계는 아래 URL에서 확인할 수 있습니다:
`https://roy8in.github.io/cb-speech-tracker/`

## 🛠 설치 및 로컬 실행

### 1. 의존성 설치
```bash
# 가상환경 생성 및 활성화 (선택)
python -m venv .venv
source .venv/bin/activate  # Windows: .venv\Scripts\activate

# 패키지 설치
pip install -r requirements.txt

# Playwright 브라우저 설치 (RBA 등 일부 스크래퍼용)
playwright install chromium
```

### 2. 연설문 수집 실행
```bash
# 최신 연설문 수집 (기본값)
$env:PYTHONPATH='.'
python -m src.collector --mode recent

# 특정 은행만 수집 (예: FRB, ECB)
python -m src.collector --banks FRB ECB

# 전체 내역 수집 (Full Mode)
python -m src.collector --mode full --start-year 2020
```

### 3. 통계 및 데이터 관리
```bash
# 대시보드용 JSON 데이터 생성
python src/generate_dashboard_data.py

# DB 통계 확인
python -m src.collector --stats

# 데이터를 CSV로 내보내기
python -m src.exporter --format csv
```

## 📂 데이터베이스 구조 (SQLite)

- **`speeches`**: 연설문 메타데이터 및 본문 (은행, 제목, 날짜, URL 등)
- **`members`**: 중앙은행 위원 정보 (이름, 역할 등)
- **`speeches_fts`**: 전문 검색(Full-Text Search) 전용 가상 테이블
- **`collection_logs`**: 수집 실행 기록 및 상태 로그

## 🤖 자동화 설정 (GitHub Actions)

이 프로젝트는 `.github/workflows/scrape.yml` 설정을 통해 매일 자동으로 실행됩니다. 수집된 데이터(`.db`)와 통계(`.json`)는 저장소에 자동으로 커밋되어 데이터가 지속적으로 축적됩니다.

## ⚖️ 라이선스
이 프로젝트는 교육 및 연구 목적으로 제작되었습니다. 수집된 데이터의 저작권은 각 중앙은행에 있습니다.
