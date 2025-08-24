#!/usr/bin/env python3
"""
feeds.yaml의 모든 피드를 사용하여 Scrapy로 RSS 스크래핑 실행
"""

import sys
import os
import logging
from pathlib import Path

# 프로젝트 루트를 Python 경로에 추가
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings
from redfin_scraper.spiders.rss_feed import RssFeedSpider

def main():
    print("🚀 Scrapy를 사용한 전체 RSS 피드 스크래핑 시작")
    
    # 로깅 설정
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s [%(name)s] %(levelname)s: %(message)s'
    )
    
    try:
        # 프로젝트 설정 가져오기
        settings = get_project_settings()
        print(f"✅ 설정 로드 완료: {settings.get('BOT_NAME')}")
        
        # 로깅 레벨 설정
        settings.set('LOG_LEVEL', 'INFO')
        print("✅ 로깅 레벨을 INFO로 설정")
        
        # 출력 파일 설정
        timestamp = os.popen('date +%Y%m%d_%H%M%S').read().strip()
        output_file = f"feeds_output_{timestamp}.jsonl"
        settings.set('FEEDS', {
            output_file: {
                'format': 'jsonlines',
                'encoding': 'utf8',
                'fields': ['guid', 'source', 'title', 'link', 'pub_date', 'description', 'author']
            }
        })
        print(f"✅ 출력 파일 설정: {output_file}")
        
        # 프로세스 생성
        process = CrawlerProcess(settings)
        print("✅ CrawlerProcess 생성 완료")
        
        # RSS 피드 스파이더 추가
        process.crawl(RssFeedSpider)
        print("✅ RSS 피드 스파이더 추가 완료")
        
        # 실행
        print("🔄 스파이더 실행 시작...")
        process.start()
        print("✅ 스파이더 실행 완료")
        
        # 결과 확인
        if os.path.exists(output_file):
            file_size = os.path.getsize(output_file)
            print(f"📁 출력 파일 크기: {file_size} bytes")
            
            if file_size > 0:
                # 파일 내용 일부 확인
                with open(output_file, 'r', encoding='utf-8') as f:
                    lines = f.readlines()
                    print(f"📊 수집된 아이템 수: {len(lines)}")
                    
                    if lines:
                        print("📄 첫 번째 아이템:")
                        print(lines[0][:200] + "...")
            else:
                print("⚠️  출력 파일이 비어있습니다.")
        else:
            print("❌ 출력 파일이 생성되지 않았습니다.")
        
    except Exception as e:
        print(f"❌ 오류 발생: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
