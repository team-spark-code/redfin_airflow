# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html

import json
import sqlite3
from datetime import datetime
from typing import Dict, Any, Optional
import hashlib
import logging
from urllib.parse import urlparse
import mysql.connector
from mysql.connector import Error

from .items import RawRSSItem, ProcessedItem, PublicItem

logger = logging.getLogger(__name__)

# 100. MariaDB 원본 RSS 데이터 저장
class MariaDBPipeline:
    """MariaDB: RSS 원본 전체 저장"""
    
    def __init__(self):
        self.conn = None
        self.cursor = None
        
    def open_spider(self, spider):
        """스파이더 시작 시 MariaDB 연결"""
        try:
            self.conn = mysql.connector.connect(
                host="localhost",
                port=3306,
                user="redfin",
                password="Redfin7620!",
                database="redfin",
                charset="utf8mb4"
            )
            self.cursor = self.conn.cursor()
            logger.info("✅ MariaDB 연결 성공")
        except Error as e:
            logger.error(f"❌ MariaDB 연결 실패: {e}")
            raise
        
    def close_spider(self, spider):
        """스파이더 종료 시 DB 연결 해제"""
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.commit()
            self.conn.close()
            logger.info("🔌 MariaDB 연결 해제")
            
    def process_item(self, item: RawRSSItem, spider) -> RawRSSItem:
        """원본 RSS 데이터를 MariaDB에 저장"""
        if not isinstance(item, RawRSSItem):
            return item
            
        # 수집 시간 추가
        item['collected_at'] = datetime.now().isoformat()
        
        try:
            # MariaDB에 저장
            sql = '''
                INSERT INTO raw_rss_items (
                    guid, source, title, link, comments, atom_link_alternate,
                    atom_link_related, atom_link_self, feedburner_orig_link,
                    pub_date, updated, dc_creator, author, description,
                    content_encoded, category, dc_subject, enclosure_url,
                    enclosure_type, enclosure_length, media_content,
                    media_thumbnail, media_keywords, copyright,
                    creative_commons_license, source_feed, slash_comments,
                    collected_at, etag, last_modified, raw_xml, response_headers
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    title = VALUES(title),
                    description = VALUES(description),
                    content_encoded = VALUES(content_encoded),
                    updated_at = CURRENT_TIMESTAMP
            '''
            
            values = (
                item.get('guid'), item.get('source'), item.get('title'),
                item.get('link'), item.get('comments'), item.get('atom_link_alternate'),
                item.get('atom_link_related'), item.get('atom_link_self'), item.get('feedburner_orig_link'),
                item.get('pub_date'), item.get('updated'), item.get('dc_creator'), item.get('author'),
                item.get('description'), item.get('content_encoded'), item.get('category'), item.get('dc_subject'),
                item.get('enclosure_url'), item.get('enclosure_type'), item.get('enclosure_length'),
                item.get('media_content'), item.get('media_thumbnail'), item.get('media_keywords'),
                item.get('copyright'), item.get('creative_commons_license'), item.get('source_feed'),
                item.get('slash_comments'), item.get('collected_at'), item.get('etag'),
                item.get('last_modified'), item.get('raw_xml'), item.get('response_headers')
            )
            
            self.cursor.execute(sql, values)
            self.conn.commit()
            
            logger.info(f"💾 RSS 아이템 저장 성공: {item.get('title', 'Unknown')[:50]}...")
            
        except Error as e:
            logger.error(f"❌ RSS 아이템 저장 실패: {e}")
            self.conn.rollback()
            
        return item

# 200. 데이터 변환 파이프라인 (기존 유지)
class DataTransformationPipeline:
    """데이터 변환 및 정제"""
    
    def process_item(self, item, spider):
        # 데이터 정제 로직
        if 'title' in item:
            item['title'] = item['title'].strip()
        if 'description' in item:
            item['description'] = item['description'].strip()
        return item

# 300. 분석 데이터 저장 (기존 유지)
class ProcessorPipeline:
    """분석된 데이터 저장"""
    
    def process_item(self, item, spider):
        # 분석 로직 (향후 구현)
        return item

# 400. 공개 API 데이터 저장 (기존 유지)
class PublicAPIPipeline:
    """공개 API용 데이터 저장"""
    
    def process_item(self, item, spider):
        # 공개용 데이터 필터링 (향후 구현)
        return item

# 기존 SQLite 파이프라인 (백업용)
class CollectorPipeline:
    """Collector Layer: RSS 원본 전체 저장 (SQLite)"""
    
    def __init__(self):
        self.conn = None
        self.cursor = None
        
    def open_spider(self, spider):
        """스파이더 시작 시 DB 연결"""
        self.conn = sqlite3.connect('raw_rss_data.db')
        self.cursor = self.conn.cursor()
        self._create_raw_table()
        
    def close_spider(self, spider):
        """스파이더 종료 시 DB 연결 해제"""
        if self.conn:
            self.conn.commit()
            self.conn.close()
            
    def _create_raw_table(self):
        """원본 데이터 저장 테이블 생성"""
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS raw_rss_items (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                guid TEXT UNIQUE,
                source TEXT,
                title TEXT,
                link TEXT,
                comments TEXT,
                atom_link_alternate TEXT,
                atom_link_related TEXT,
                atom_link_self TEXT,
                feedburner_orig_link TEXT,
                pub_date TEXT,
                updated TEXT,
                dc_creator TEXT,
                author TEXT,
                description TEXT,
                content_encoded TEXT,
                category TEXT,
                dc_subject TEXT,
                enclosure_url TEXT,
                enclosure_type TEXT,
                enclosure_length TEXT,
                media_content TEXT,
                media_thumbnail TEXT,
                media_keywords TEXT,
                copyright TEXT,
                creative_commons_license TEXT,
                source_feed TEXT,
                slash_comments TEXT,
                collected_at TEXT,
                etag TEXT,
                last_modified TEXT,
                raw_xml TEXT,
                response_headers TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
    def process_item(self, item: RawRSSItem, spider) -> RawRSSItem:
        """원본 RSS 데이터 저장"""
        if not isinstance(item, RawRSSItem):
            return item
            
        # 수집 시간 추가
        item['collected_at'] = datetime.now().isoformat()
        
        # DB에 저장
        self.cursor.execute('''
            INSERT OR REPLACE INTO raw_rss_items (
                guid, source, title, link, comments, atom_link_alternate,
                atom_link_related, atom_link_self, feedburner_orig_link,
                pub_date, updated, dc_creator, author, description,
                content_encoded, category, dc_subject, enclosure_url,
                enclosure_type, enclosure_length, media_content,
                media_thumbnail, media_keywords, copyright,
                creative_commons_license, source_feed, slash_comments,
                collected_at, etag, last_modified, raw_xml, response_headers
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            item.get('guid'), item.get('source'), item.get('title'),
            item.get('link'), item.get('comments'), item.get('atom_link_alternate'),
            item.get('atom_link_related'), item.get('atom_link_self'), item.get('feedburner_orig_link'),
            item.get('pub_date'), item.get('updated'), item.get('dc_creator'), item.get('author'),
            item.get('description'), item.get('content_encoded'), item.get('category'), item.get('dc_subject'),
            item.get('enclosure_url'), item.get('enclosure_type'), item.get('enclosure_length'),
            item.get('media_content'), item.get('media_thumbnail'), item.get('media_keywords'),
            item.get('copyright'), item.get('creative_commons_license'), item.get('source_feed'),
            item.get('slash_comments'), item.get('collected_at'), item.get('etag'),
            item.get('last_modified'), item.get('raw_xml'), item.get('response_headers')
        ))
        
        return item
