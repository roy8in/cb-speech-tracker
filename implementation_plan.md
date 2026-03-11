# BOC Scraper 수정 계획

Bank of Canada 스피치 수집이 실패하는 원인을 분석하고 수정합니다.

## 문제 분석

현재 [boc.py](file:///Users/binmoojin/work/cb-speech-tracker/src/scrapers/boc.py)의 문제점:

1. **불필요한 Playwright 사용**: BOC 웹사이트는 JavaScript 렌더링 없이 일반 HTTP 요청(`requests`)으로 충분히 콘텐츠를 가져올 수 있음 → **ECB/FRB처럼 `self._get()` 사용으로 전환**
2. **날짜 파싱 로직 오류**: URL에서 날짜를 추출하는 regex 패턴(`/speeches/YYYY/MM/DD/`)이 실제 BOC URL 패턴(`/YYYY/MM/slug/`)과 다름
3. **스피치 링크 필터링 부정확**: `/press/speeches/`를 포함하는 링크를 찾지만, 실제 스피치 URL 패턴은 `/YYYY/MM/slug/` 형태
4. **스피커 추출 없음**: 현재 `speaker`가 항상 `None`

## 실제 BOC 웹사이트 구조 (조사 결과)

### 목록 페이지 (`/press/speeches/`)
- 각 스피치는 `<h3>` 태그 안의 `<a>` 링크로 표시
- **스피치 URL 패턴**: `https://www.bankofcanada.ca/YYYY/MM/slug/` (텍스트 스피치)
- **멀티미디어 URL 패턴**: `https://www.bankofcanada.ca/multimedia/slug/` (웹캐스트 - 수집 불필요)
- **스피커**: `/profile/` 링크로 제공 (예: `<a href="/profile/tiff-macklem/">Tiff Macklem</a>`)
- **날짜**: 스피치 항목 부근의 텍스트로 표시
- **페이지네이션**: `?mt_page=N` 파라미터 사용

### 상세 페이지
- **본문 콘텐츠**: `.page-content` 또는 `.post-content` 내의 `<p>`, `<h2>`, `<h3>` 태그
- **제외 대상**: `nav`, `script`, `style`, `footer`, `header`, `.related-info`, `aside`

## Proposed Changes

### Scrapers

#### [MODIFY] [boc.py](file:///Users/binmoojin/work/cb-speech-tracker/src/scrapers/boc.py)

**주요 변경사항:**

1. **Playwright 제거 → `requests` 기반으로 전환**
   - [_get_playwright()](file:///Users/binmoojin/work/cb-speech-tracker/src/scrapers/boc.py#19-33) 메서드 삭제
   - `self._get()` (BaseScraper 제공) 사용

2. **[fetch_speech_list()](file:///Users/binmoojin/work/cb-speech-tracker/src/scrapers/base.py#66-83) 완전 재작성**
   - URL 필터링: `href*="/profile/"` → 스피커, `/YYYY/MM/` 패턴 → 스피치 날짜
   - `/multimedia/` URL 제외 (웹캐스트는 텍스트 스피치가 아님)
   - `<h3>` 안의 `<a>` 태그에서 제목과 URL 추출
   - 스피커 이름은 동일 블록 내 `/profile/` 링크에서 추출
   - URL에서 날짜 추출 (예: `/2026/03/slug/` → `2026-03`)
   - 페이지네이션 지원 (`?mt_page=N`)

3. **[fetch_speech_text()](file:///Users/binmoojin/work/cb-speech-tracker/src/scrapers/boc.py#79-91) 수정**
   - `self._get()` 사용으로 변경 (이미 일부 사용 중이나 셀렉터 업데이트)
   - `.page-content` 또는 `.post-content` 셀렉터 사용
   - 불필요 요소 제거: `nav`, `script`, `style`, `footer`, `header`, `aside`, `.related-info`

4. **[get_all_speeches()](file:///Users/binmoojin/work/cb-speech-tracker/src/scrapers/base.py#93-113) 오버라이드**
   - BOC는 연도별 URL이 아닌 페이지네이션 방식이므로 별도 구현 필요

## Verification Plan

### Manual Verification

1. 터미널에서 아래 스크립트 실행하여 스피치 목록이 정상 수집되는지 확인:
```python
python -c "
from src.scrapers.boc import BOCScraper
scraper = BOCScraper()
speeches = scraper.fetch_speech_list()
for s in speeches[:5]:
    print(s)
print(f'Total: {len(speeches)}')
"
```

2. 스피치 텍스트 수집 확인:
```python
python -c "
from src.scrapers.boc import BOCScraper
scraper = BOCScraper()
speeches = scraper.fetch_speech_list()
if speeches:
    text = scraper.fetch_speech_text(speeches[0]['url'])
    print(f'Title: {speeches[0][\"title\"]}')
    print(f'Text length: {len(text) if text else 0}')
    print(f'First 200 chars: {text[:200] if text else \"None\"}')
"
```

> [!IMPORTANT]
> Playwright 의존성은 [requirements.txt](file:///Users/binmoojin/work/cb-speech-tracker/requirements.txt)에 남겨둡니다 (다른 스크래퍼가 사용할 가능성 대비). BOC에서만 사용하지 않도록 변경합니다.
