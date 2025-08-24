import scrapy
import json
from pathlib import Path
from typing import Optional, List, Dict, Any
from ..items import PublicItem
from datetime import datetime
import os

class NaverNewsSpider(scrapy.Spider):
    name = "naver_news"
    
    def __init__(self, *args, **kwargs):
        super(NaverNewsSpider, self).__init__(*args, **kwargs)
        # naver_ifrs17.json 파일 경로
        self.naver_data_file = os.getenv('NAVER_DATA_FILE', '/opt/airflow/naver_scraper/naver_scraper/naver_ifrs17.json')

    def start_requests(self):
        """네이버 뉴스 데이터 파일에서 링크를 읽어 요청 생성"""
        try:
            if not Path(self.naver_data_file).exists():
                self.logger.warning(f"네이버 뉴스 데이터 파일이 없습니다: {self.naver_data_file}")
                return
            
            with open(self.naver_data_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
                
            # 네이버 뉴스 링크 추출
            news_links = self._extract_news_links(data)
            
            for link in news_links:
                yield scrapy.Request(
                    url=link,
                    callback=self.parse_naver_news,
                    meta={'source': 'naver_news'},
                    dont_filter=True,
                    headers={
                        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8'
                    }
                )
                
        except FileNotFoundError:
            self.logger.error(f"네이버 뉴스 데이터 파일을 찾을 수 없습니다: {self.naver_data_file}")
        except json.JSONDecodeError as e:
            self.logger.error(f"JSON 파싱 실패: {e}")
        except Exception as e:
            self.logger.error(f"파일 읽기 실패: {e}")

    def _extract_news_links(self, data: Dict[str, Any]) -> List[str]:
        """데이터에서 네이버 뉴스 링크 추출"""
        links = []
        
        def extract_links_recursive(obj):
            if isinstance(obj, dict):
                for key, value in obj.items():
                    if key == 'link' and isinstance(value, str) and 'news.naver.com' in value:
                        links.append(value)
                    elif key == 'url' and isinstance(value, str) and 'news.naver.com' in value:
                        links.append(value)
                    else:
                        extract_links_recursive(value)
            elif isinstance(obj, list):
                for item in obj:
                    extract_links_recursive(item)
        
        extract_links_recursive(data)
        return list(set(links))  # 중복 제거

    def parse_naver_news(self, response):
        """네이버 뉴스 파싱 및 PublicItem 생성"""
        try:
            # 제목 추출
            title = self._extract_title(response)
            
            # 본문 추출
            content = self._extract_content(response)
            
            # 요약 생성
            summary = self._generate_summary(content, title)
            
            # 태그 추출
            tags = self._extract_tags(response)
            
            # 저자 정보 추출
            authors = self._extract_authors(response)
            
            # PublicItem 생성
            public_item = PublicItem(
                source='naver_news',
                title=title,
                link=response.url,
                published=datetime.now(),  # 네이버 뉴스는 발행일을 정확히 파악하기 어려움
                summary=summary,
                authors=authors,
                tags=tags
            )
            
            yield public_item
            
        except Exception as e:
            self.logger.error(f"네이버 뉴스 파싱 실패: {e}")

    def _extract_title(self, response) -> str:
        """제목 추출"""
        # 다양한 제목 선택자 시도
        title_selectors = [
            'h1::text',
            '.article_title::text',
            '.news_title::text',
            '.title::text',
            'h2::text',
            'meta[property="og:title"]::attr(content)',
            'title::text'
        ]
        
        for selector in title_selectors:
            title = response.css(selector).get()
            if title and title.strip():
                return title.strip()
        
        return "제목 없음"

    def _extract_content(self, response) -> str:
        """본문 내용 추출"""
        # 네이버 뉴스 본문 선택자들
        content_selectors = [
            '.article_body',
            '.news_body',
            '.article_content',
            '.content',
            'article',
            '.post-content',
            '.entry-content'
        ]
        
        content_text = ""
        
        for selector in content_selectors:
            elements = response.css(selector)
            if elements:
                # 불필요한 요소 제거
                for element in elements:
                    element.remove('script')
                    element.remove('style')
                    element.remove('nav')
                    element.remove('header')
                    element.remove('footer')
                    element.remove('.advertisement')
                    element.remove('.sidebar')
                    element.remove('.related_news')
                    element.remove('.comment_area')
                
                text = elements.get()
                if text:
                    content_text = text.strip()
                    break
        
        # 본문을 찾지 못한 경우 p 태그들에서 텍스트 수집
        if not content_text:
            paragraphs = response.css('p::text').getall()
            if paragraphs:
                content_text = ' '.join([p.strip() for p in paragraphs if p.strip()])
        
        return content_text

    def _generate_summary(self, content: str, title: str) -> str:
        """요약 생성"""
        if content:
            # 첫 200자로 요약
            summary = content[:200].strip()
            if len(content) > 200:
                summary += "..."
            return summary
        elif title:
            return title
        else:
            return ""

    def _extract_tags(self, response) -> List[str]:
        """태그 추출"""
        tags = []
        
        # 메타 태그에서 키워드 추출
        keywords = response.css('meta[name="keywords"]::attr(content)').get()
        if keywords:
            tags.extend([tag.strip() for tag in keywords.split(',')])
        
        # 메타 태그에서 description 추출하여 키워드로 사용
        description = response.css('meta[name="description"]::attr(content)').get()
        if description:
            # 간단한 키워드 추출 (공백으로 구분된 단어들)
            words = description.split()
            for word in words:
                if len(word) > 2 and word.isalpha():
                    tags.append(word.lower())
        
        # 중복 제거 및 정리
        tags = list(set([tag for tag in tags if tag.strip()]))
        
        return tags[:10]  # 최대 10개 태그만 반환

    def _extract_authors(self, response) -> List[str]:
        """저자 정보 추출"""
        authors = []
        
        # 네이버 뉴스에서 저자 정보를 찾는 선택자들
        author_selectors = [
            '.author::text',
            '.reporter::text',
            '.writer::text',
            '.byline::text',
            'meta[name="author"]::attr(content)'
        ]
        
        for selector in author_selectors:
            author = response.css(selector).get()
            if author and author.strip():
                authors.append(author.strip())
        
        # 중복 제거
        return list(set(authors))
