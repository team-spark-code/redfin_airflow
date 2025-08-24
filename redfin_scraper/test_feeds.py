#!/usr/bin/env python3
"""
feeds.yaml을 사용하여 RSS 데이터 스크랩 테스트
"""

import yaml
import feedparser
import json
from datetime import datetime
from pathlib import Path

def load_feeds_config():
    """feeds.yaml 파일을 로드합니다."""
    feeds_path = Path("feeds/feeds.yaml")
    
    try:
        with open(feeds_path, "r", encoding="utf-8") as f:
            config = yaml.safe_load(f)
            print(f"✅ feeds.yaml 로드 완료: {len(config.get('feeds', []))}개 피드")
            return config
    except FileNotFoundError:
        print(f"❌ feeds.yaml 파일을 찾을 수 없습니다: {feeds_path}")
        return None
    except Exception as e:
        print(f"❌ feeds.yaml 파싱 오류: {e}")
        return None

def scrape_rss_feed(name, url):
    """개별 RSS 피드를 스크랩합니다."""
    print(f"\n📡 스크랩 중: {name} -> {url}")
    
    try:
        # RSS 피드 파싱
        feed = feedparser.parse(url)
        
        if feed.bozo:
            print(f"⚠️  피드 파싱 경고: {feed.bozo_exception}")
        
        print(f"   📊 피드 제목: {feed.feed.get('title', 'Unknown')}")
        print(f"   📝 엔트리 수: {len(feed.entries)}")
        
        # 첫 번째 몇 개 엔트리만 출력
        entries = []
        for i, entry in enumerate(feed.entries[:3]):  # 처음 3개만
            entry_data = {
                "title": entry.get("title", ""),
                "link": entry.get("link", ""),
                "published": entry.get("published", ""),
                "summary": entry.get("summary", "")[:100] + "..." if entry.get("summary") else "",
                "author": entry.get("author", ""),
                "tags": [tag.term for tag in entry.get("tags", [])] if hasattr(entry, 'tags') else []
            }
            entries.append(entry_data)
            print(f"   📄 {i+1}. {entry_data['title'][:60]}...")
        
        return {
            "feed_name": name,
            "feed_url": url,
            "feed_title": feed.feed.get("title", ""),
            "entry_count": len(feed.entries),
            "sample_entries": entries,
            "scraped_at": datetime.now().isoformat()
        }
        
    except Exception as e:
        print(f"   ❌ 스크랩 실패: {e}")
        return {
            "feed_name": name,
            "feed_url": url,
            "error": str(e),
            "scraped_at": datetime.now().isoformat()
        }

def main():
    print("🚀 feeds.yaml RSS 스크래핑 테스트 시작")
    
    # feeds.yaml 로드
    config = load_feeds_config()
    if not config:
        return
    
    # 테스트용으로 처음 5개 피드만 사용
    test_feeds = config.get("feeds", [])[:5]
    print(f"\n🔬 테스트용 {len(test_feeds)}개 피드 선택")
    
    # 각 피드 스크랩
    results = []
    for feed in test_feeds:
        name = feed.get("name", "Unknown")
        url = feed.get("url", "")
        group = feed.get("group", "unknown")
        
        print(f"\n📋 피드 정보: {name} ({group})")
        result = scrape_rss_feed(name, url)
        results.append(result)
    
    # 결과 저장
    output_file = f"rss_test_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)
    
    print(f"\n💾 결과가 {output_file}에 저장되었습니다.")
    
    # 요약 출력
    successful = sum(1 for r in results if "error" not in r)
    total = len(results)
    print(f"\n📊 스크래핑 요약: {successful}/{total} 성공")

if __name__ == "__main__":
    main()
