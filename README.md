# CB Speech Tracker (Central Bank Speech Archive)

중앙은행(FRB, ECB, BOE, BOJ, RBA)의 연설문을 자동으로 수집하고, SQLite FTS5를 이용해 초고속 전문 검색을 지원하는 아카이브 시스템입니다.

## 🚀 주요 기능

- **자동 수집 파이프라인**: GitHub Actions를 통해 매일 한국 시간 새벽 3시에 최신 연설문을 자동으로 수집합니다.
- **모니터링 대시보드**: 수집 현황, 시스템 상태, 화자 통계 등을 웹(GitHub Pages)에서 한눈에 확인할 수 있습니다.
- **봇 차단 우회**: Playwright(Chromium)를 활용하여 RBA 등 보안이 강력한 사이트에서도 안정적으로 데이터를 가져옵니다.
- **고성능 검색 (FTS5)**: 수천 건의 연설문 본문을 대상으로 특정 키워드(예: AI, Inflation)를 0.1초 내에 검색합니다.
- **증분 업데이트**: 이미 수집된 URL은 건너뛰고 새로운 연설문만 스마트하게 추가하여 DB를 유지합니다.

## 📊 모니터링 대시보드

수집 상태와 통계는 아래 URL에서 확인할 수 있습니다:
`https://roy8in.github.io/cb-speech-tracker/`

## 🛠 설치 및 로컬 실행

```bash
# 의존성 설치
pip install -r requirements.txt

# Playwright 브라우저 설치 (RBA 수집용)
playwright install chromium

# 로컬에서 수집 실행
python3 main.py

# 대시보드용 데이터 생성
python3 src/generate_dashboard_data.py
```

## 📂 데이터베이스 구조 (SQLite)

- **`speeches`**: 원본 데이터 (은행 코드, 화자, 제목, 날짜, URL, 본문 전문)
- **`speeches_fts`**: 전문 검색(Full-Text Search) 전용 가상 테이블
- **`collection_logs`**: 수집 실행 기록 및 에러 로그

## 🤖 자동화 설정 (GitHub Actions)

이 프로젝트는 `.github/workflows/scrape.yml` 설정을 통해 매일 자동으로 실행됩니다. 수집된 데이터(`.db`)와 통계(`.json`)는 저장소에 자동으로 커밋되어 데이터가 유실되지 않고 계속 축적됩니다.

## ⚖️ 라이선스
이 프로젝트는 교육 및 연구 목적으로 제작되었습니다. 수집된 데이터의 저작권은 각 중앙은행에 있습니다.
