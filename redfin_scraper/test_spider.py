import scrapy

class TestSpider(scrapy.Spider):
    name = "test"
    
    def start_requests(self):
        self.logger.info("테스트 스파이더 시작!")
        yield scrapy.Request(
            "https://httpbin.org/get",
            callback=self.parse,
            meta={"test": True}
        )
    
    def parse(self, response):
        self.logger.info(f"응답 받음: {response.status}")
        self.logger.info(f"응답 내용: {response.text[:200]}...")
        
        # 간단한 아이템 생성
        yield {
            "url": response.url,
            "status": response.status,
            "content_length": len(response.text)
        }
