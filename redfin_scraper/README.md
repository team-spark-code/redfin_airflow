# Redfin Scraper - hit_rss 이식 프로젝트

이 프로젝트는 `hit_rss` 프로젝트를 Airflow DAG로 이식한 것입니다. RSS 피드 수집 및 본문 추출을 위한 자동화된 워크플로우를 제공합니다.

## 🏗️ 프로젝트 구조

```
redfin_scraper/
├── dags/                          # Airflow DAG 파일들
│   └── hit_rss_dag.py            # hit_rss 스크래핑 DAG
├── redfin_scraper/               # Scrapy 프로젝트
│   ├── spiders/                  # 스파이더들
│   │   ├── rss_feed.py          # RSS 피드 수집
│   │   ├── article_extractor.py # 본문 추출
│   │   └── naver_news.py        # 네이버 뉴스 수집
│   ├── items.py                  # 데이터 모델
│   ├── pipelines.py              # 데이터 처리 파이프라인
│   ├── middlewares.py            # 미들웨어
│   └── settings.py               # Scrapy 설정
├── feeds/                        # RSS 피드 설정
│   └── feeds.yaml               # 피드 목록 및 설정
├── requirements.txt              # Python 의존성
└── README.md                     # 이 파일
```

## 🚀 주요 기능

### 1. RSS 피드 수집 (`rss_feed` 스파이더)
- 다양한 RSS 피드에서 뉴스 아이템 수집
- Dublin Core 메타데이터 지원
- Atom 링크 및 미디어 정보 처리
- 캐시 헤더 (ETag, Last-Modified) 지원

### 2. 본문 추출 (`article_extractor` 스파이더)
- RSS 피드에서 링크를 읽어 본문 추출
- 다양한 웹사이트 구조에 대응하는 선택자
- 콘텐츠 유형 자동 분류
- 언어 감지 및 가독성 점수 계산

### 3. 네이버 뉴스 수집 (`naver_news` 스파이더)
- 네이버 뉴스 데이터 파일에서 링크 추출
- 네이버 뉴스 본문 파싱
- 태그 및 저자 정보 추출

### 4. Airflow DAG 통합
- 6시간마다 자동 실행
- 단계별 태스크 실행 및 모니터링
- 데이터 통합 및 요약 생성
- 에러 처리 및 재시도 로직

## 📋 데이터 모델

### RawRSSItem
- RSS 피드 원본 데이터 저장
- Dublin Core 메타데이터 포함
- 수집 메타데이터 (시간, 헤더 등)

### ProcessedItem
- 분석된 데이터 (태그, 감정, 가독성 등)
- 임베딩 벡터 지원
- 콘텐츠 유형 및 언어 분류

### PublicItem
- 외부 공개용 데이터
- 민감한 정보 제거
- 표준화된 형식

## ⚙️ 설정

### 환경 변수
```bash
SCRAPY_PROJECT_DIR=/opt/airflow/scrapy_projects/redfin_scraper
DATA_OUTPUT_DIR=/opt/airflow/data/out
FEEDS_CONFIG_PATH=/opt/airflow/feeds/feeds.yaml
NAVER_DATA_FILE=/opt/airflow/naver_scraper/naver_scraper/naver_ifrs17.json
```

### feeds.yaml 설정
- RSS 피드 URL 목록
- 업데이트 간격 및 타임아웃 설정
- 키워드 필터링 옵션
- 언어 및 카테고리 필터

## 🚀 실행 방법

### 1. 의존성 설치
```bash
cd redfin_airflow/scrapy_projects/redfin_scraper
pip install -r requirements.txt
```

### 2. Airflow DAG 활성화
- Airflow UI에서 `hit_rss_scraping` DAG 활성화
- DAG는 6시간마다 자동 실행

### 3. 수동 실행
```bash
# RSS 피드 수집
scrapy crawl rss_feed -o output.jsonl

# 본문 추출
scrapy crawl article_extractor -a input=input.jsonl -o articles.jsonl

# 네이버 뉴스 수집
scrapy crawl naver_news -o naver_news.jsonl
```

## 📊 출력 데이터

### RSS 피드 수집 결과
- `rss_feed_YYYYMMDD_HHMMSS.jsonl`: RSS 피드 원본 데이터

### 본문 추출 결과
- `articles_YYYYMMDD_HHMMSS.jsonl`: 추출된 본문 및 분석 데이터

### 네이버 뉴스 결과
- `naver_articles_YYYYMMDD_HHMMSS.jsonl`: 네이버 뉴스 데이터

### 통합 결과
- `all_articles_YYYYMMDD_HHMMSS.jsonl`: 모든 뉴스 통합 데이터
- `execution_summary_YYYYMMDD_HHMMSS.jsonl`: 실행 요약 및 통계

## 🔧 개발 및 테스트

### 코드 품질
```bash
# 코드 포맷팅
black redfin_scraper/

# 린팅
flake8 redfin_scraper/

# 타입 체크
mypy redfin_scraper/

# 테스트 실행
pytest tests/
```

### 로깅
- Scrapy 로그: `scrapy.log`
- Airflow 태스크 로그: Airflow UI에서 확인
- 실행 요약: `execution_summary_*.jsonl` 파일

## 📈 모니터링

### Airflow UI
- DAG 실행 상태 및 히스토리
- 태스크별 실행 시간 및 에러
- XCom을 통한 데이터 전달 확인

### 성능 지표
- 수집된 뉴스 수
- 실행 시간
- 에러율 및 재시도 횟수

## 🚨 문제 해결

### 일반적인 문제들
1. **RSS 피드 접근 불가**: 네트워크 설정 및 User-Agent 확인
2. **본문 추출 실패**: 웹사이트 구조 변경으로 인한 선택자 업데이트 필요
3. **메모리 부족**: CONCURRENT_REQUESTS 설정 조정

### 디버깅
```bash
# 상세 로그로 실행
scrapy crawl rss_feed -L DEBUG

# 특정 URL 테스트
scrapy shell "https://example.com/rss"
```

## 🤝 기여

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests if applicable
5. Submit a pull request

## 📄 라이선스

이 프로젝트는 MIT 라이선스 하에 배포됩니다.

## 📞 연락처

- 프로젝트 관리자: sungminwoo.devops@gmail.com
- 이슈 리포트: GitHub Issues 사용
