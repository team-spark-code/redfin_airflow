from __future__ import annotations
import scrapy
import re
from typing import List, Dict, Optional, Any, Union
from datetime import datetime
from pydantic import BaseModel, Field, validator, HttpUrl, ValidationError
from enum import Enum


# 열거형 정의
class ContentType(str, Enum):
    """콘텐츠 유형 열거형"""
    NEWS = "news"
    ARTICLE = "article"
    BLOG = "blog"
    PRESS_RELEASE = "press_release"
    INTERVIEW = "interview"
    OPINION = "opinion"

class Language(str, Enum):
    """언어 열거형"""
    KOREAN = "ko"
    ENGLISH = "en"


# ========= 1) Raw 레이어: 텍스트-only 스키마 (Dublin Core 옵션 유지) =========
class RawRSSItem(BaseModel):
    """
    Collector Layer: RSS 원본 저장 (텍스트-only 기준)
    - Dublin Core(dc_*)는 옵션으로 유지(상호운용/폴백) 
    """
    # 기본 식별
    guid: str = Field(..., description="고유 식별자")
    source: str = Field(..., description="RSS 피드 소스")
    title: str = Field(..., description="제목")
    link: HttpUrl = Field(..., description="원본 링크")
    
    # 시각/저자
    pub_date: Optional[datetime] = Field(None, description="RSS pubDate")
    updated: Optional[datetime] = Field(None, description="Atom updated")
    dc_creator: Optional[str] = Field(None, description="dc:creator")
    author: Optional[List[str]] = Field(None, description="author (여러 명일 수 있음)")
    
    # 본문
    description: Optional[str] = Field(None, description="요약 (description)")
    content_encoded: Optional[str] = Field(None, description="HTML 전체 본문 (content:encoded)")
    
    # 카테고리/태그
    category: Optional[List[str]] = Field(None, description="category (여러 개)")
    dc_subject: Optional[List[str]] = Field(None, description="dc:subject")
    
    # 저작권/라이선스
    copyright: Optional[str] = Field(None, description="copyright")
    creative_commons_license: Optional[str] = Field(None, description="creativeCommons:license")
    
    # 메타/캐시
    source_feed: Optional[str] = Field(None, description="source (원본 피드 참조)")
    slash_comments: Optional[int] = Field(None, ge=0, description="slash:comments (댓글 수)")
    
    # 수집 메타
    collected_at: datetime = Field(..., description="수집 시간")
    etag: Optional[str] = Field(None, description="ETag 캐시")
    last_modified: Optional[str] = Field(None, description="Last-Modified 캐시")
    raw_xml: Optional[str] = Field(None, description="원본 XML 보존")
    response_headers: Optional[Dict[str, str]] = Field(None, description="응답 헤더 정보")

    class Config:
        """Pydantic 설정"""
        arbitrary_types_allowed = True                      # Scrapy와 호환성을 위한 설정
        anystr_strip_whitespace = True                      # 공백 정리
        json_encoders = {datetime: lambda v: v.isoformat()} # JSON 직렬화 시 datetime을 ISO 형식으로 변환


class ProcessedItem(BaseModel):
    """
    Processor Layer: 내부 DB에서 분석된 데이터
    - 기본 정보는 RawRSSItem에서 복사
    """
    guid: str = Field(..., description="고유 식별자")
    source: str = Field(..., description="소스")
    title: str = Field(..., description="제목")
    link: HttpUrl = Field(..., description="링크")
    
    # 분석된 데이터
    text_embedding: Optional[List[float]] = Field(None, description="텍스트 임베딩 벡터")
    summary_embedding: Optional[List[float]] = Field(None, description="요약 임베딩 벡터")
    extracted_tags: Optional[List[str]] = Field(None, description="추출된 태그")
    sentiment_score: Optional[float] = Field(None, ge=-1.0, le=1.0, description="감정 분석 점수")
    content_type: Optional[ContentType] = Field(None, description="콘텐츠 유형 분류")
    language: Optional[Language] = Field(None, description="언어 감지")
    readability_score: Optional[float] = Field(None, ge=0.0, le=100.0, description="가독성 점수")
    key_entities: Optional[List[Dict[str, Any]]] = Field(None, description="주요 엔티티 추출")
    processed_at: datetime = Field(..., description="처리 시간")

    class Config:
        arbitrary_types_allowed = True
        anystr_strip_whitespace = True
        json_encoders = {datetime: lambda v: v.isoformat()}


# ========= 2) Public Layer: 외부 공개용 스키마 (API/웹) =========
class PublicItem(BaseModel):
    """
    Public Layer: 외부 공개용 데이터 (API, 웹 등)
    - 민감한 정보 제거, 표준화된 형식
    """
    source: str = Field(..., description="뉴스 소스")
    title: str = Field(..., description="뉴스 제목")
    link: HttpUrl = Field(..., description="뉴스 링크")
    published: Optional[datetime] = Field(None, description="발행일")
    summary: Optional[str] = Field(None, description="뉴스 요약")
    authors: Optional[List[str]] = Field(None, description="저자 목록")
    tags: Optional[List[str]] = Field(None, description="태그 목록")

    class Config:
        arbitrary_types_allowed = True
        anystr_strip_whitespace = True
        json_encoders = {datetime: lambda v: v.isoformat()}


# ========= 3) Scrapy Item 클래스 (하위 호환성) =========
class RawRSSScrapyItem(scrapy.Item):
    """Scrapy Item 클래스 (RawRSSItem과 동일한 구조)"""
    guid = scrapy.Field()
    source = scrapy.Field()
    title = scrapy.Field()
    link = scrapy.Field()
    pub_date = scrapy.Field()
    updated = scrapy.Field()
    dc_creator = scrapy.Field()
    author = scrapy.Field()
    description = scrapy.Field()
    content_encoded = scrapy.Field()
    category = scrapy.Field()
    dc_subject = scrapy.Field()
    copyright = scrapy.Field()
    creative_commons_license = scrapy.Field()
    source_feed = scrapy.Field()
    slash_comments = scrapy.Field()
    collected_at = scrapy.Field()
    etag = scrapy.Field()
    last_modified = scrapy.Field()
    raw_xml = scrapy.Field()
    response_headers = scrapy.Field()


class ProcessedScrapyItem(scrapy.Item):
    """Scrapy Item 클래스 (ProcessedItem과 동일한 구조)"""
    guid = scrapy.Field()
    source = scrapy.Field()
    title = scrapy.Field()
    link = scrapy.Field()
    text_embedding = scrapy.Field()
    summary_embedding = scrapy.Field()
    extracted_tags = scrapy.Field()
    sentiment_score = scrapy.Field()
    content_type = scrapy.Field()
    language = scrapy.Field()
    readability_score = scrapy.Field()
    key_entities = scrapy.Field()
    processed_at = scrapy.Field()


class PublicScrapyItem(scrapy.Item):
    """Scrapy Item 클래스 (PublicItem과 동일한 구조)"""
    source = scrapy.Field()
    title = scrapy.Field()
    link = scrapy.Field()
    published = scrapy.Field()
    summary = scrapy.Field()
    authors = scrapy.Field()
    tags = scrapy.Field()


# ========= 4) 유틸리티 함수 =========
def convert_to_scrapy_item(pydantic_item: Union[RawRSSItem, ProcessedItem, PublicItem]) -> Union[RawRSSScrapyItem, ProcessedScrapyItem, PublicScrapyItem]:
    """Pydantic 모델을 Scrapy Item으로 변환"""
    if isinstance(pydantic_item, RawRSSItem):
        scrapy_item = RawRSSScrapyItem()
        for field, value in pydantic_item.dict().items():
            scrapy_item[field] = value
        return scrapy_item
    
    elif isinstance(pydantic_item, ProcessedItem):
        scrapy_item = ProcessedScrapyItem()
        for field, value in pydantic_item.dict().items():
            scrapy_item[field] = value
        return scrapy_item
    
    elif isinstance(pydantic_item, PublicItem):
        scrapy_item = PublicScrapyItem()
        for field, value in pydantic_item.dict().items():
            scrapy_item[field] = value
        return scrapy_item
    
    else:
        raise ValueError(f"지원하지 않는 아이템 타입: {type(pydantic_item)}")


def convert_from_scrapy_item(scrapy_item: Union[RawRSSScrapyItem, ProcessedScrapyItem, PublicScrapyItem]) -> Union[RawRSSItem, ProcessedItem, PublicItem]:
    """Scrapy Item을 Pydantic 모델로 변환"""
    item_dict = dict(scrapy_item)
    
    if isinstance(scrapy_item, RawRSSScrapyItem):
        return RawRSSItem(**item_dict)
    
    elif isinstance(scrapy_item, ProcessedScrapyItem):
        return ProcessedItem(**item_dict)
    
    elif isinstance(scrapy_item, PublicScrapyItem):
        return PublicItem(**item_dict)
    
    else:
        raise ValueError(f"지원하지 않는 Scrapy Item 타입: {type(scrapy_item)}")


# ========= 5) 검증 및 변환 함수 =========
def validate_rss_item(item_data: Dict[str, Any]) -> RawRSSItem:
    """RSS 아이템 데이터 검증 및 RawRSSItem 생성"""
    try:
        return RawRSSItem(**item_data)
    except ValidationError as e:
        raise ValueError(f"RSS 아이템 검증 실패: {e}")


def validate_processed_item(item_data: Dict[str, Any]) -> ProcessedItem:
    """처리된 아이템 데이터 검증 및 ProcessedItem 생성"""
    try:
        return ProcessedItem(**item_data)
    except ValidationError as e:
        raise ValueError(f"처리된 아이템 검증 실패: {e}")


def validate_public_item(item_data: Dict[str, Any]) -> PublicItem:
    """공개 아이템 데이터 검증 및 PublicItem 생성"""
    try:
        return PublicItem(**item_data)
    except ValidationError as e:
        raise ValueError(f"공개 아이템 검증 실패: {e}")


# ========= 6) 기본값 및 상수 =========
DEFAULT_CONTENT_TYPE = ContentType.NEWS
DEFAULT_LANGUAGE = Language.ENGLISH
DEFAULT_READABILITY_SCORE = 50.0
DEFAULT_SENTIMENT_SCORE = 0.0

# 필수 필드 목록
REQUIRED_RSS_FIELDS = ['guid', 'source', 'title', 'link', 'collected_at']
REQUIRED_PROCESSED_FIELDS = ['guid', 'source', 'title', 'link', 'processed_at']
REQUIRED_PUBLIC_FIELDS = ['source', 'title', 'link']
