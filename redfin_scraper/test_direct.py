#!/usr/bin/env python3
"""
직접 Scrapy 테스트 스크립트
"""

import sys
import os
import logging
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings
from redfin_scraper.spiders.rss_feed import RssFeedSpider

def main():
    print("🚀 Scrapy 직접 실행 테스트 시작")
    
    # 로깅 설정
    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s [%(name)s] %(levelname)s: %(message)s'
    )
    
    try:
        # 프로젝트 설정 가져오기
        settings = get_project_settings()
        print(f"✅ 설정 로드 완료: {settings.get('BOT_NAME')}")
        
        # 로깅 레벨 설정
        settings.set('LOG_LEVEL', 'DEBUG')
        print("✅ 로깅 레벨을 DEBUG로 설정")
        
        # 프로세스 생성
        process = CrawlerProcess(settings)
        print("✅ CrawlerProcess 생성 완료")
        
        # 스파이더 추가
        process.crawl(RssFeedSpider)
        print("✅ RSS 피드 스파이더 추가 완료")
        
        # 실행
        print("🔄 스파이더 실행 시작...")
        process.start()
        print("✅ 스파이더 실행 완료")
        
    except Exception as e:
        print(f"❌ 오류 발생: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
