# -*- coding: utf-8 -*-
"""
뉴스 사이트를 직접 크롤링하는 스파이더
"""
import scrapy
from datetime import datetime


class NewsSpider(scrapy.Spider):
    name = 'news_spider'
    
    def __init__(self, news_sites=None, *args, **kwargs):
        super(NewsSpider, self).__init__(*args, **kwargs)
        # 뉴스 사이트들을 쉼표로 구분된 문자열로 받음
        self.news_sites = news_sites.split(',') if news_sites else [
            'https://techcrunch.com',
            'https://www.theverge.com'
        ]
        self.start_urls = self.news_sites
        
    def parse(self, response):
        """메인 페이지에서 기사 링크 추출"""
        try:
            # 기사 링크 추출 (사이트별로 다른 선택자 사용)
            if 'techcrunch.com' in response.url:
                article_links = response.css('h2 a::attr(href)').getall()
            elif 'theverge.com' in response.url:
                article_links = response.css('h2 a::attr(href)').getall()
            else:
                # 기본 선택자
                article_links = response.css('a[href*="/article/"], a[href*="/news/"]::attr(href)').getall()
            
            # 절대 URL로 변환
            for link in article_links[:5]:  # 최근 5개 기사만
                if link.startswith('/'):
                    full_url = response.urljoin(link)
                else:
                    full_url = link
                    
                yield scrapy.Request(
                    url=full_url,
                    callback=self.parse_article,
                    meta={'source_site': response.url}
                )
                
        except Exception as e:
            self.logger.error(f"링크 추출 오류: {e}")
    
    def parse_article(self, response):
        """기사 페이지 파싱"""
        try:
            # 사이트별 파싱 로직
            if 'techcrunch.com' in response.url:
                title = response.css('h1::text').get('').strip()
                content = ' '.join(response.css('p::text').getall()[:10])  # 첫 10개 문단
                author = response.css('.author-name::text').get('').strip()
                
            elif 'theverge.com' in response.url:
                title = response.css('h1::text').get('').strip()
                content = ' '.join(response.css('p::text').getall()[:10])
                author = response.css('.author-name::text').get('').strip()
                
            else:
                # 기본 파싱
                title = response.css('h1::text, h2::text').get('').strip()
                content = ' '.join(response.css('p::text').getall()[:10])
                author = ''
            
            # 결과 반환
            if title and content:
                yield {
                    'url': response.url,
                    'title': title,
                    'content': content[:1000],  # 첫 1000자만
                    'author': author,
                    'source_site': response.meta['source_site'],
                    'crawled_at': datetime.now().isoformat()
                }
                
        except Exception as e:
            self.logger.error(f"기사 파싱 오류: {e}")
