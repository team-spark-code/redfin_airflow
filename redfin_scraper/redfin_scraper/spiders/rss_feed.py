import yaml
import feedparser
import scrapy
from datetime import datetime
from dateutil import parser as dtp
from pathlib import Path
from ..items import RawRSSItem, ProcessedItem, PublicItem, ContentType, Language
import json
from urllib.parse import urlparse
from typing import Optional, List, Dict, Any
import os

class RssFeedSpider(scrapy.Spider):
    name = "rss_feed"

    def start_requests(self):
        # 환경변수에서 feeds.yaml 경로 가져오기
        feeds_path = os.getenv('FEEDS_CONFIG_PATH', 'feeds/feeds.yaml')
        
        self.logger.info(f"🔍 feeds.yaml 경로: {feeds_path}")
        
        try:
            with open(feeds_path, "r", encoding="utf-8") as f:
                cfg = yaml.safe_load(f) or {}
                self.logger.info(f"✅ feeds.yaml 로드 완료: {len(cfg.get('feeds', []))}개 피드")
        except FileNotFoundError:
            self.logger.warning(f"feeds.yaml 파일을 찾을 수 없습니다: {feeds_path}")
            # 기본 RSS 피드 사용
            cfg = {
                "feeds": [
                    {"name": "test_feed", "url": "https://httpbin.org/xml"},
                    {"name": "sample_rss", "url": "https://feeds.bbci.co.uk/news/rss.xml"}
                ]
            }
            self.logger.info(f"🔄 기본 피드 사용: {len(cfg.get('feeds', []))}개")
        
        for src in cfg.get("feeds", []):
            url = src["url"]
            self.logger.info(f"📡 요청 생성: {src.get('name', 'Unknown')} -> {url}")
            yield scrapy.Request(
                url,
                callback=self.parse_feed,
                meta={"source": src.get("name") or url},
                dont_filter=True,
                headers={
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                    'Accept': 'application/rss+xml, application/atom+xml, application/xml, text/xml'
                }
            )

    def parse_feed(self, response):
        """RSS 피드 파싱 및 RawRSSItem 생성"""
        parsed = feedparser.parse(response.body)
        source = response.meta["source"]
        
        # 응답 헤더 정보 저장 (바이트를 문자열로 변환)
        response_headers = {}
        for key, value in response.headers.items():
            if isinstance(value, list):
                response_headers[key.decode('utf-8') if isinstance(key, bytes) else key] = [v.decode('utf-8') if isinstance(v, bytes) else v for v in value]
            else:
                response_headers[key.decode('utf-8') if isinstance(key, bytes) else key] = value.decode('utf-8') if isinstance(value, bytes) else value
        
        for e in parsed.entries:
            try:
                # 기본 식별 정보
                guid = e.get("id") or e.get("guid") or e.get("link")
                if not guid:
                    self.logger.warning(f"GUID not found for entry: {e.get('title', 'Unknown')}")
                    continue
                
                # 날짜 파싱
                pub_date = self._parse_datetime(e.get("published") or e.get("updated") or e.get("pubDate"))
                updated = self._parse_datetime(e.get("updated"))
                
                # 저자 정보 처리
                authors = self._extract_authors(e)
                
                # 카테고리 정보 처리
                categories = self._extract_categories(e)
                dc_subjects = self._extract_dc_subjects(e)
                
                # 미디어 정보 처리
                enclosure_info = self._extract_enclosure_info(e)
                media_info = self._extract_media_info(e)
                
                # 링크를 절대 URL로 변환
                link = e.get("link", "")
                if link and not link.startswith(('http://', 'https://')):
                    link = response.urljoin(link)
                
                # RawRSSItem 생성
                raw_item = RawRSSItem(
                    # 기본 식별 정보
                    guid=guid,
                    source=source,
                    title=e.get("title", ""),
                    link=link,
                    comments=e.get("comments"),
                    
                    # Atom 링크 정보
                    atom_link_alternate=self._extract_atom_link(e, "alternate"),
                    atom_link_related=self._extract_atom_link(e, "related"),
                    atom_link_self=self._extract_atom_link(e, "self"),
                    feedburner_orig_link=e.get("feedburner_origlink"),
                    
                    # 시각 및 저자 정보
                    pub_date=pub_date,
                    updated=updated,
                    dc_creator=e.get("dc_creator"),
                    author=authors,
                    
                    # 본문 정보
                    description=(e.get("summary") or e.get("description") or "").strip(),
                    content_encoded=self._extract_content_encoded(e),
                    
                    # 카테고리 및 태그
                    category=categories,
                    dc_subject=dc_subjects,
                    
                    # 미디어 정보
                    enclosure_url=enclosure_info.get("url"),
                    enclosure_type=enclosure_info.get("type"),
                    enclosure_length=enclosure_info.get("length"),
                    media_content=media_info.get("content"),
                    media_thumbnail=media_info.get("thumbnail"),
                    
                    # 저작권 및 라이선스
                    copyright=e.get("rights"),
                    creative_commons_license=e.get("creativeCommons:license"),
                    
                    # 메타 정보
                    source_feed=e.get("source"),
                    slash_comments=self._parse_int(e.get("slash:comments")),
                    
                    # 수집 메타
                    collected_at=datetime.now(),
                    etag=response_headers.get("etag"),
                    last_modified=response_headers.get("last-modified"),
                    raw_xml=response.text,
                    response_headers=response_headers
                )
                
                yield raw_item
                
            except Exception as e:
                self.logger.error(f"Entry 파싱 실패: {e}")
                if hasattr(e, 'get') and callable(e.get):
                    self.logger.error(f"Entry 정보: {e.get('title', 'Unknown')}")

    def _parse_datetime(self, date_str: Optional[str]) -> Optional[datetime]:
        """날짜 문자열을 datetime 객체로 파싱"""
        if not date_str:
            return None
        
        try:
            return dtp.parse(date_str)
        except Exception:
            self.logger.warning(f"날짜 파싱 실패: {date_str}")
            return None

    def _extract_authors(self, entry: Dict[str, Any]) -> List[str]:
        """저자 정보 추출"""
        authors = []
        
        # author 필드 처리
        if "author" in entry:
            if isinstance(entry["author"], str):
                authors.append(entry["author"])
            elif isinstance(entry["author"], dict) and "name" in entry["author"]:
                authors.append(entry["author"]["name"])
        
        # dc:creator 처리
        if "dc_creator" in entry:
            dc_creator = entry["dc_creator"]
            if isinstance(dc_creator, str):
                authors.append(dc_creator)
            elif isinstance(dc_creator, list):
                authors.extend(dc_creator)
        
        return list(set(authors))  # 중복 제거

    def _extract_categories(self, entry: Dict[str, Any]) -> List[str]:
        """카테고리 정보 추출"""
        categories = []
        
        if "category" in entry:
            category = entry["category"]
            if isinstance(category, str):
                categories.append(category)
            elif isinstance(category, list):
                categories.extend(category)
        
        return categories

    def _extract_dc_subjects(self, entry: Dict[str, Any]) -> List[str]:
        """dc:subject 정보 추출"""
        subjects = []
        
        if "dc_subject" in entry:
            dc_subject = entry["dc_subject"]
            if isinstance(dc_subject, str):
                subjects.append(dc_subject)
            elif isinstance(dc_subject, list):
                subjects.extend(dc_subject)
        
        return subjects

    def _extract_enclosure_info(self, entry: Dict[str, Any]) -> Dict[str, Any]:
        """enclosure 정보 추출"""
        enclosure_info = {}
        
        if "enclosures" in entry:
            enclosures = entry["enclosures"]
            if enclosures and len(enclosures) > 0:
                enclosure = enclosures[0]
                enclosure_info = {
                    "url": enclosure.get("href"),
                    "type": enclosure.get("type"),
                    "length": enclosure.get("length")
                }
        
        return enclosure_info

    def _extract_media_info(self, entry: Dict[str, Any]) -> Dict[str, Any]:
        """미디어 정보 추출"""
        media_info = {}
        
        if "media_content" in entry:
            media_content = entry["media_content"]
            if media_content and len(media_content) > 0:
                media = media_content[0]
                media_info["content"] = media.get("url")
        
        if "media_thumbnail" in entry:
            media_thumbnail = entry["media_thumbnail"]
            if media_thumbnail and len(media_thumbnail) > 0:
                thumbnail = media_thumbnail[0]
                media_info["thumbnail"] = thumbnail.get("url")
        
        return media_info

    def _extract_atom_link(self, entry: Dict[str, Any], rel: str) -> Optional[str]:
        """Atom 링크 정보 추출"""
        if "links" in entry:
            for link in entry["links"]:
                if link.get("rel") == rel:
                    return link.get("href")
        return None

    def _extract_content_encoded(self, entry: Dict[str, Any]) -> Optional[str]:
        """content:encoded 정보 추출"""
        return entry.get("content", [{}])[0].get("value") if "content" in entry else None

    def _parse_int(self, value: Any) -> Optional[int]:
        """정수값 파싱"""
        try:
            return int(value) if value else None
        except (ValueError, TypeError):
            return None
