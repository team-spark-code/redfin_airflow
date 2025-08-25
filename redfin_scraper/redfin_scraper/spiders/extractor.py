import scrapy
import json
from pathlib import Path
from typing import Optional, List, Dict, Any
from datetime import datetime
import os

class ArticleExtractorSpider(scrapy.Spider):
    name = "extractor"
    
    def __init__(self, input_file: Optional[str] = None, *args, **kwargs):
        super(ArticleExtractorSpider, self).__init__(*args, **kwargs)
        self.input_file = input_file or os.getenv('RSS_INPUT_FILE')
        
        if not self.input_file:
            raise ValueError("input_file parameter or RSS_INPUT_FILE environment variable is required")

    def start_requests(self):
        """RSS feed file에서 링크를 읽어 본문 추출 요청 생성"""
        try:
            with open(self.input_file, 'r', encoding='utf-8') as f:
                for line_num, line in enumerate(f, 1):
                    try:
                        data = json.loads(line.strip())
                        link = data.get('link')
                        
                        if link and isinstance(link, str):
                            yield scrapy.Request(
                                url=link,
                                callback=self.parse_article,
                                meta={
                                    'rss_data': data,
                                    'line_number': line_num
                                },
                                dont_filter=True,
                                headers={
                                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                                    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8'
                                }
                            )
                        else:
                            self.logger.warning(f"Invalid link: {link}")
                            
                    except json.JSONDecodeError as e:
                        self.logger.error(f"JSON parsing failed (line {line_num}): {e}")
                    except Exception as e:
                        self.logger.error(f"Line {line_num} processing failed: {e}")
                        
        except FileNotFoundError:
            self.logger.error(f"Input file not found: {self.input_file}")
            raise
        except Exception as e:
            self.logger.error(f"File reading failed: {e}")
            raise

    def parse_article(self, response):
        """기사 본문 파싱 및 ProcessedItem 생성"""
        rss_data = response.meta['rss_data']
        line_number = response.meta['line_number']
        
        try:
            # 본문 텍스트 추출
            article_text = self._extract_article_text(response)
            
            if not article_text:
                self.logger.warning(f"No article text found for line {line_number}: {rss_data.get('link')}")
            
            # 요약 생성 (간단한 버전)
            summary = self._generate_summary(article_text, rss_data.get('description', ''))
            
            # 태그 추출
            tags = self._extract_tags(response, rss_data)
            
            # 콘텐츠 유형 분류
            content_type = self._classify_content_type(rss_data, article_text)
            
            # 언어 감지
            language = self._detect_language(article_text, rss_data)
            
            # Simple dictionary instead of complex ProcessedItem
            processed_item = {
                'guid': rss_data.get('guid', ''),
                'source': rss_data.get('source', ''),
                'title': rss_data.get('title', ''),
                'link': rss_data.get('link', ''),
                'article_text': article_text,
                'summary': summary,
                'tags': tags,
                'content_type': content_type,
                'language': language,
                'readability_score': self._calculate_readability(article_text),
                'key_entities': self._extract_entities(article_text),
                'processed_at': datetime.now().isoformat(),
                'text_length': len(article_text) if article_text else 0
            }
            
            yield processed_item
            
            # Public item (simplified version)
            public_item = {
                'type': 'public',
                'source': rss_data.get('source', ''),
                'title': rss_data.get('title', ''),
                'link': rss_data.get('link', ''),
                'published': rss_data.get('pub_date'),
                'summary': summary,
                'authors': rss_data.get('author', []),
                'tags': tags
            }
            
            yield public_item
            
        except Exception as e:
            self.logger.error(f"Article parsing failed (line {line_number}): {e}")
            # Still yield a basic item with error info
            yield {
                'error': True,
                'error_message': str(e),
                'link': rss_data.get('link', ''),
                'line_number': line_number
            }

    def _extract_article_text(self, response) -> str:
        """기사 본문 텍스트 추출 - FIXED VERSION"""
        # 일반적인 기사 본문 선택자들
        selectors = [
            'article',
            '.article-content',
            '.post-content',
            '.entry-content',
            '.content',
            'main',
            '.main-content',
            '#content',
            '.story-body',
            '.article-body'
        ]
        
        article_text = ""
        
        # 선택자로 본문 찾기
        for selector in selectors:
            elements = response.css(selector)
            if elements:
                text_parts = []
                for element in elements:
                    # FIXED: Proper way to remove unwanted elements
                    unwanted_selectors = [
                        'script', 'style', 'nav', 'header', 'footer',
                        '.advertisement', '.sidebar', '.ad'
                    ]
                    
                    for unwanted in unwanted_selectors:
                        for unwanted_elem in element.css(unwanted):
                            unwanted_elem.remove()
                    
                    # FIXED: Extract actual text, not HTML
                    text_nodes = element.css('p::text, div::text, span::text').getall()
                    clean_texts = [text.strip() for text in text_nodes if text.strip()]
                    
                    if clean_texts:
                        text_parts.extend(clean_texts)
                
                if text_parts:
                    article_text = ' '.join(text_parts)
                    break
        
        # 본문을 찾지 못한 경우 전체 페이지에서 주요 텍스트 추출
        if not article_text:
            # p 태그들의 텍스트를 수집
            paragraphs = response.css('p::text').getall()
            if paragraphs:
                article_text = ' '.join([p.strip() for p in paragraphs if p.strip()])
        
        # Clean up text
        if article_text:
            article_text = ' '.join(article_text.split())  # Remove extra whitespace
        
        return article_text

    def _generate_summary(self, article_text: str, rss_description: str) -> str:
        """요약 생성"""
        if rss_description and len(rss_description.strip()) > 0:
            return rss_description.strip()
        
        # 간단한 요약 생성 (첫 200자)
        if article_text:
            summary = article_text[:200].strip()
            if len(article_text) > 200:
                summary += "..."
            return summary
        
        return ""

    def _extract_tags(self, response, rss_data: Dict[str, Any]) -> List[str]:
        """태그 추출"""
        tags = []
        
        # RSS 데이터에서 태그 가져오기
        if rss_data.get('category'):
            if isinstance(rss_data['category'], list):
                tags.extend(rss_data['category'])
            else:
                tags.append(rss_data['category'])
        
        if rss_data.get('dc_subject'):
            if isinstance(rss_data['dc_subject'], list):
                tags.extend(rss_data['dc_subject'])
            else:
                tags.append(rss_data['dc_subject'])
        
        # HTML에서 메타 태그로 태그 찾기
        meta_tags = response.css('meta[name="keywords"]::attr(content)').get()
        if meta_tags:
            tags.extend([tag.strip() for tag in meta_tags.split(',')])
        
        # 중복 제거 및 정리
        tags = list(set([tag.strip() for tag in tags if tag.strip()]))
        
        return tags

    def _classify_content_type(self, rss_data: Dict[str, Any], article_text: str) -> str:
        """콘텐츠 유형 분류"""
        title = rss_data.get('title', '').lower()
        description = rss_data.get('description', '').lower()
        
        # 키워드 기반 분류
        if any(word in title or word in description for word in ['interview', '인터뷰']):
            return 'INTERVIEW'
        elif any(word in title or word in description for word in ['opinion', 'opinion', '칼럼', '기고']):
            return 'OPINION'
        elif any(word in title or word in description for word in ['press release', '보도자료', '공지']):
            return 'PRESS_RELEASE'
        elif any(word in title or word in description for word in ['blog', '블로그']):
            return 'BLOG'
        else:
            return 'NEWS'

    def _detect_language(self, article_text: str, rss_data: Dict[str, Any]) -> str:
        """언어 감지"""
        # 간단한 한글/영어 감지
        if not article_text:
            return 'UNKNOWN'
            
        korean_chars = sum(1 for char in article_text if '\uac00' <= char <= '\ud7af')
        english_chars = sum(1 for char in article_text if char.isalpha() and ord(char) < 128)
        
        if korean_chars > english_chars:
            return 'KOREAN'
        else:
            return 'ENGLISH'

    def _calculate_readability(self, text: str) -> float:
        """가독성 점수 계산 (간단한 버전)"""
        if not text:
            return 0.0
        
        # 문장 수 계산
        sentences = [s.strip() for s in text.split('.') if s.strip()]
        sentence_count = len(sentences)
        
        # 단어 수 계산
        words = text.split()
        word_count = len(words)
        
        # 평균 단어 길이
        if word_count > 0:
            avg_word_length = sum(len(word) for word in words) / word_count
        else:
            avg_word_length = 0
        
        # 간단한 가독성 점수 (0-100)
        if sentence_count > 0 and word_count > 0:
            score = max(0, 100 - (avg_word_length * 2) - (word_count / sentence_count * 0.5))
            return min(100, score)
        
        return 50.0

    def _extract_entities(self, text: str) -> List[Dict[str, Any]]:
        """주요 엔티티 추출 (간단한 버전)"""
        entities = []
        
        if not text:
            return entities
        
        # 간단한 패턴 매칭으로 엔티티 찾기
        import re
        
        # 이메일
        emails = re.findall(r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b', text)
        for email in emails:
            entities.append({
                'type': 'email',
                'value': email,
                'confidence': 0.9
            })
        
        # URL
        urls = re.findall(r'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\\(\\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+', text)
        for url in urls:
            entities.append({
                'type': 'url',
                'value': url,
                'confidence': 0.8
            })
        
        return entities