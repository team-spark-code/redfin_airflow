# -*- coding: utf-8 -*-
"""
RSS 피드에서 기사 URL을 추출하고 각 기사를 크롤링하는 스파이더
"""
import scrapy
from scrapy.spiders import CrawlSpider, Rule
from scrapy.linkextractors import LinkExtractor
import feedparser
import json
from datetime import datetime


class RssArticleSpider(CrawlSpider):
    name = 'rss_article_spider'
    
    def __init__(self, rss_urls=None, *args, **kwargs):
        super(RssArticleSpider, self).__init__(*args, **kwargs)
        # RSS URL들을 쉼표로 구분된 문자열로 받음
        self.rss_urls = rss_urls.split(',') if rss_urls else [
            'https://feeds.feedburner.com/TechCrunch',
            'https://rss.cnn.com/rss/edition.rss'
        ]
        self.start_urls = []
        self.article_urls = set()
        
    def start_requests(self):
        """RSS 피드들을 파싱하여 기사 URL 추출"""
        for rss_url in self.rss_urls:
            yield scrapy.Request(
                url=rss_url,
                callback=self.parse_rss,
                meta={'rss_url': rss_url}
            )
    
    def parse_rss(self, response):
        """RSS 피드 파싱"""
        try:
            # feedparser로 RSS 파싱
            feed = feedparser.parse(response.text)
            
            for entry in feed.entries[:10]:  # 최근 10개 기사만
                article_url = entry.get('link')
                if article_url and article_url not in self.article_urls:
                    self.article_urls.add(article_url)
                    yield scrapy.Request(
                        url=article_url,
                        callback=self.parse_article,
                        meta={
                            'title': entry.get('title', ''),
                            'published': entry.get('published', ''),
                            'author': entry.get('author', ''),
                            'source': entry.get('source', {}).get('title', ''),
                            'rss_url': response.meta['rss_url']
                        }
                    )
        except Exception as e:
            self.logger.error(f"RSS 파싱 오류: {e}")
    
    def parse_article(self, response):
        """기사 페이지 파싱"""
        try:
            # 기본 메타데이터
            title = response.meta.get('title', '')
            published = response.meta.get('published', '')
            author = response.meta.get('author', '')
            source = response.meta.get('source', '')
            rss_url = response.meta.get('rss_url', '')
            
            # 본문 추출 (간단한 CSS 선택자)
            content = ' '.join(response.css('p::text, h1::text, h2::text, h3::text').getall())
            
            # 결과 반환
            yield {
                'url': response.url,
                'title': title,
                'content': content[:1000],  # 첫 1000자만
                'author': author,
                'source': source,
                'published_at': published,
                'rss_url': rss_url,
                'crawled_at': datetime.now().isoformat()
            }
            
        except Exception as e:
            self.logger.error(f"기사 파싱 오류: {e}")
