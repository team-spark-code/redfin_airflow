#!/usr/bin/env python3
"""
feeds.yaml의 모든 RSS 피드를 안정적으로 스크랩
"""

import yaml
import feedparser
import json
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Any
import requests

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

def scrape_rss_feed(name: str, url: str, group: str = "unknown") -> Dict[str, Any]:
    """개별 RSS 피드를 스크랩합니다."""
    print(f"\n📡 스크랩 중: {name} ({group})")
    print(f"   🔗 URL: {url}")
    
    try:
        # RSS 피드 파싱
        feed = feedparser.parse(url)
        
        if feed.bozo:
            print(f"   ⚠️  피드 파싱 경고: {feed.bozo_exception}")
        
        feed_title = feed.feed.get('title', 'Unknown')
        entry_count = len(feed.entries)
        
        print(f"   📊 피드 제목: {feed_title}")
        print(f"   📝 엔트리 수: {entry_count}")
        
        # 모든 엔트리 처리
        entries = []
        for entry in feed.entries:
            # 링크를 절대 URL로 변환
            link = entry.get("link", "")
            if link and not link.startswith(('http://', 'https://')):
                # 상대 URL을 절대 URL로 변환 (간단한 방법)
                if url.endswith('/'):
                    link = url + link.lstrip('/')
                else:
                    link = url + '/' + link.lstrip('/')
            
            entry_data = {
                "guid": entry.get("id") or entry.get("guid") or link,
                "source": name,
                "title": entry.get("title", ""),
                "link": link,
                "pub_date": entry.get("published") or entry.get("updated") or entry.get("pubDate", ""),
                "description": entry.get("summary") or entry.get("description", ""),
                "author": entry.get("author", ""),
                "category": entry.get("category", ""),
                "tags": [tag.term for tag in entry.get("tags", [])] if hasattr(entry, 'tags') else [],
                "group": group,
                "scraped_at": datetime.now().isoformat()
            }
            entries.append(entry_data)
        
        # 샘플 출력 (처음 3개)
        for i, entry in enumerate(entries[:3]):
            print(f"   📄 {i+1}. {entry['title'][:60]}...")
        
        return {
            "feed_name": name,
            "feed_url": url,
            "feed_title": feed_title,
            "group": group,
            "entry_count": entry_count,
            "entries": entries,
            "scraped_at": datetime.now().isoformat(),
            "status": "success"
        }
        
    except Exception as e:
        print(f"   ❌ 스크랩 실패: {e}")
        return {
            "feed_name": name,
            "feed_url": url,
            "group": group,
            "error": str(e),
            "scraped_at": datetime.now().isoformat(),
            "status": "error"
        }

def main():
    print("🚀 feeds.yaml 전체 RSS 피드 스크래핑 시작")
    start_time = time.time()
    
    # feeds.yaml 로드
    config = load_feeds_config()
    if not config:
        return
    
    feeds = config.get("feeds", [])
    print(f"\n🔬 총 {len(feeds)}개 피드 스크래핑 시작")
    
    # 각 피드 스크랩
    results = []
    successful_feeds = 0
    total_entries = 0
    
    for i, feed in enumerate(feeds, 1):
        name = feed.get("name", f"Unknown_{i}")
        url = feed.get("url", "")
        group = feed.get("group", "unknown")
        
        print(f"\n📋 [{i}/{len(feeds)}] 피드 정보: {name} ({group})")
        
        result = scrape_rss_feed(name, url, group)
        results.append(result)
        
        if result["status"] == "success":
            successful_feeds += 1
            total_entries += result["entry_count"]
        
        # 요청 간격 조절 (서버 부하 방지)
        if i < len(feeds):
            time.sleep(1)
    
    # 전체 결과 저장
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    
    # 요약 결과
    summary = {
        "scraping_summary": {
            "total_feeds": len(feeds),
            "successful_feeds": successful_feeds,
            "failed_feeds": len(feeds) - successful_feeds,
            "total_entries": total_entries,
            "start_time": datetime.fromtimestamp(start_time).isoformat(),
            "end_time": datetime.now().isoformat(),
            "duration_seconds": time.time() - start_time
        },
        "feed_results": results
    }
    
    summary_file = f"feeds_summary_{timestamp}.json"
    with open(summary_file, "w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)
    
    # 모든 엔트리를 하나의 파일로 저장
    all_entries = []
    for result in results:
        if result["status"] == "success" and "entries" in result:
            all_entries.extend(result["entries"])
    
    entries_file = f"all_entries_{timestamp}.jsonl"
    with open(entries_file, "w", encoding="utf-8") as f:
        for entry in all_entries:
            f.write(json.dumps(entry, ensure_ascii=False) + "\n")
    
    # 최종 요약 출력
    print(f"\n" + "="*60)
    print("🎉 RSS 스크래핑 완료!")
    print(f"📊 전체 피드: {len(feeds)}개")
    print(f"✅ 성공: {successful_feeds}개")
    print(f"❌ 실패: {len(feeds) - successful_feeds}개")
    print(f"📝 총 엔트리: {total_entries}개")
    print(f"⏱️  소요 시간: {time.time() - start_time:.1f}초")
    print(f"💾 요약 파일: {summary_file}")
    print(f"📄 엔트리 파일: {entries_file}")
    print("="*60)

if __name__ == "__main__":
    main()
