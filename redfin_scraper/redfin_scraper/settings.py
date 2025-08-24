# Scrapy settings for redfin_scraper project
#
# For simplicity, this file contains only settings considered important or
# commonly used. You can find more settings consulting the documentation:
#
#     https://docs.scrapy.org/en/latest/topics/settings.html
#     https://docs.scrapy.org/en/latest/topics/downloader-middleware.html
#     https://docs.scrapy.org/en/latest/topics/spider-middleware.html
import os
from pathlib import Path

# 환경변수에서 정수값 안전하게 처리
def safe_int(value, default=0):
    """환경변수에서 정수값을 안전하게 변환"""
    try:
        return int(value) if value else default
    except (ValueError, TypeError):
        return default


# ===============================
# 기본 설정 (2025-08-25)
# ===============================

BOT_NAME = 'redfin_scraper'

SPIDER_MODULES = ['redfin_scraper.spiders']
NEWSPIDER_MODULE = 'redfin_scraper.spiders'

ADDONS = {}

# 사용자 에이전트 식별 (웹사이트 식별)
USER_AGENT = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'

# robots.txt 규칙 준수
ROBOTSTXT_OBEY = True

# 동시성 및 쓰로틀링 설정
# CONCURRENT_REQUESTS = 8
CONCURRENT_REQUESTS_PER_DOMAIN = safe_int(os.getenv("CONCURRENT_REQUESTS_PER_DOMAIN"), 1)
DOWNLOAD_DELAY = safe_int(os.getenv("DOWNLOAD_DELAY"), 1)
RETRY_ENABLED = True
RETRY_TIMES = safe_int(os.getenv("RETRY_TIMES"), 2)  # 정수값으로 명시
DEFAULT_REQUEST_HEADERS = {
    "User-Agent": "redfin-scraper/1.0 (+contact: sungminwoo.devops@gmail.com)"
}

# 결과 경로
ROOT = Path(__file__).resolve().parents[2]
FEEDS = {
    str(ROOT / "data/out/ai_news.jsonl"): {
        "format": "jsonlines",
        "encoding": "utf8",
        "fields": ["source","title","link","published","summary","authors","tags"],
    }
}

# 로깅 설정
LOG_LEVEL = 'INFO'

# 피드 설정 (Airflow에서 오버라이드됨)
FEEDS = {}

# ===============================
# hit_rss 프로젝트 이식 설정
# ===============================

# RSS 피드 설정
RSS_FEEDS_CONFIG = os.getenv('FEEDS_CONFIG_PATH', '/opt/airflow/feeds/feeds.yaml')

# 데이터 출력 디렉토리
DATA_OUTPUT_DIR = os.getenv('DATA_OUTPUT_DIR', '/opt/airflow/data/out')

# 네이버 뉴스 데이터 파일 경로
NAVER_DATA_FILE = os.getenv('NAVER_DATA_FILE', '/opt/airflow/naver_scraper/naver_scraper/naver_ifrs17.json')

# 스파이더별 설정
SPIDER_SETTINGS = {
    'rss_feed': {
        'DOWNLOAD_DELAY': 2,
        'CONCURRENT_REQUESTS_PER_DOMAIN': 1,
        'RETRY_TIMES': 3,
    },
    'article_extractor': {
        'DOWNLOAD_DELAY': 1,
        'CONCURRENT_REQUESTS_PER_DOMAIN': 2,
        'RETRY_TIMES': 2,
    },
    'naver_news': {
        'DOWNLOAD_DELAY': 1,
        'CONCURRENT_REQUESTS_PER_DOMAIN': 2,
        'RETRY_TIMES': 2,
    }
}

# 캐시 설정
HTTPCACHE_ENABLED = True
HTTPCACHE_EXPIRATION_SECS = 3600  # 1시간
HTTPCACHE_DIR = 'httpcache'
HTTPCACHE_IGNORE_HTTP_CODES = [500, 502, 503, 504, 408, 429]

# 다운로더 미들웨어 설정
DOWNLOADER_MIDDLEWARES = {
    'scrapy.downloadermiddlewares.useragent.UserAgentMiddleware': None,
    'scrapy.downloadermiddlewares.retry.RetryMiddleware': 90,
    'scrapy.downloadermiddlewares.httpproxy.HttpProxyMiddleware': 110,
}

# 스파이더 미들웨어 설정
SPIDER_MIDDLEWARES = {
    'scrapy.spidermiddlewares.httperror.HttpErrorMiddleware': 815,
}

# 아이템 파이프라인 설정
ITEM_PIPELINES = {
    # 'redfin_scraper.pipelines.ValidationPipeline': 300,
    # 'redfin_scraper.pipelines.DuplicatesPipeline': 400,
    # 'redfin_scraper.pipelines.StoragePipeline': 500,
}

# 로깅 설정 (hit_rss 호환)
LOG_FILE = 'scrapy.log'
LOG_FORMAT = '%(asctime)s [%(name)s] %(levelname)s: %(message)s'
LOG_DATEFORMAT = '%Y-%m-%d %H:%M:%S'

# 에러 처리 설정
DOWNLOAD_FAIL_ON_DATALOSS = False
RETRY_HTTP_CODES = [500, 502, 503, 504, 408, 429, 403]

# 타임아웃 설정
DOWNLOAD_TIMEOUT = 30
DOWNLOAD_MAXSIZE = 0  # 무제한

# 쿠키 설정
COOKIES_ENABLED = False

# 압축 설정
COMPRESSION_ENABLED = True
