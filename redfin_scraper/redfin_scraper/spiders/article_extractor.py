import scrapy
import json
from pathlib import Path
from typing import Optional, List, Dict, Any
from ..items import ProcessedItem, PublicItem, ContentType, Language
from datetime import datetime
import os

class ArticleExtractorSpider(scrapy.Spider):
    name = "article_extractor"
    
    def __init__(self, input_file: Optional[str] = None, *args, **kwargs):
        super(ArticleExtractorSpider, self).__init__(*args, **kwargs)
        self.input_file = input_file or os.getenv('RSS_INPUT_FILE')
        
        if not self.input_file:
            raise ValueError("input_file 매개변수 또는 RSS_INPUT_FILE 환경변수가 필요합니다")

    def start_requests(self):
        """RSS 피드 파일에서 링크를 읽어 본문 추출 요청 생성"""
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
                            self.logger.warning(f"유효하지 않은 링크: {link}")
                            
                    except json.JSONDecodeError as e:
                        self.logger.error(f"JSON 파싱 실패 (라인 {line_num}): {e}")
                    except Exception as e:
                        self.logger.error(f"라인 {line_num} 처리 실패: {e}")
                        
        except FileNotFoundError:
            self.logger.error(f"입력 파일을 찾을 수 없습니다: {self.input_file}")
            raise
        except Exception as e:
            self.logger.error(f"파일 읽기 실패: {e}")
            raise

    def parse_article(self, response):
        """기사 본문 파싱 및 ProcessedItem 생성"""
        rss_data = response.meta['rss_data']
        line_number = response.meta['line_number']
        
        try:
            # 본문 텍스트 추출
            article_text = self._extract_article_text(response)
            
            # 요약 생성 (간단한 버전)
            summary = self._generate_summary(article_text, rss_data.get('description', ''))
            
            # 태그 추출
            tags = self._extract_tags(response, rss_data)
            
            # 콘텐츠 유형 분류
            content_type = self._classify_content_type(rss_data, article_text)
            
            # 언어 감지
            language = self._detect_language(article_text, rss_data)
            
            # ProcessedItem 생성
            processed_item = ProcessedItem(
                guid=rss_data.get('guid', ''),
                source=rss_data.get('source', ''),
                title=rss_data.get('title', ''),
                link=rss_data.get('link', ''),
                
                # 분석된 데이터
                text_embedding=None,  # 임베딩은 별도 처리
                summary_embedding=None,
                extracted_tags=tags,
                sentiment_score=None,  # 감정 분석은 별도 처리
                content_type=content_type,
                language=language,
                readability_score=self._calculate_readability(article_text),
                key_entities=self._extract_entities(article_text),
                processed_at=datetime.now()
            )
            
            yield processed_item
            
            # PublicItem도 생성 (간소화된 버전)
            public_item = PublicItem(
                source=rss_data.get('source', ''),
                title=rss_data.get('title', ''),
                link=rss_data.get('link', ''),
                published=rss_data.get('pub_date'),
                summary=summary,
                authors=rss_data.get('author', []),
                tags=tags
            )
            
            yield public_item
            
        except Exception as e:
            self.logger.error(f"기사 파싱 실패 (라인 {line_number}): {e}")

    def _extract_article_text(self, response) -> str:
        """기사 본문 텍스트 추출"""
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
                # 텍스트 추출 및 정리
                text_parts = []
                for element in elements:
                    # 불필요한 요소 제거
                    element.remove('script')
                    element.remove('style')
                    element.remove('nav')
                    element.remove('header')
                    element.remove('footer')
                    element.remove('.advertisement')
                    element.remove('.sidebar')
                    
                    text = element.get()
                    if text:
                        text_parts.append(text.strip())
                
                if text_parts:
                    article_text = ' '.join(text_parts)
                    break
        
        # 본문을 찾지 못한 경우 전체 페이지에서 주요 텍스트 추출
        if not article_text:
            # p 태그들의 텍스트를 수집
            paragraphs = response.css('p::text').getall()
            if paragraphs:
                article_text = ' '.join([p.strip() for p in paragraphs if p.strip()])
        
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

    def _classify_content_type(self, rss_data: Dict[str, Any], article_text: str) -> ContentType:
        """콘텐츠 유형 분류"""
        title = rss_data.get('title', '').lower()
        description = rss_data.get('description', '').lower()
        text = article_text.lower()
        
        # 키워드 기반 분류
        if any(word in title or word in description for word in ['interview', '인터뷰']):
            return ContentType.INTERVIEW
        elif any(word in title or word in description for word in ['opinion', 'opinion', '칼럼', '기고']):
            return ContentType.OPINION
        elif any(word in title or word in description for word in ['press release', '보도자료', '공지']):
            return ContentType.PRESS_RELEASE
        elif any(word in title or word in description for word in ['blog', '블로그']):
            return ContentType.BLOG
        else:
            return ContentType.NEWS

    def _detect_language(self, article_text: str, rss_data: Dict[str, Any]) -> Language:
        """언어 감지"""
        # 간단한 한글/영어 감지
        korean_chars = sum(1 for char in article_text if '\uac00' <= char <= '\ud7af')
        english_chars = sum(1 for char in article_text if char.isalpha() and ord(char) < 128)
        
        if korean_chars > english_chars:
            return Language.KOREAN
        else:
            return Language.ENGLISH

    def _calculate_readability(self, text: str) -> float:
        """가독성 점수 계산 (간단한 버전)"""
        if not text:
            return 0.0
        
        # 문장 수 계산
        sentences = text.split('.')
        sentence_count = len([s for s in sentences if s.strip()])
        
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
