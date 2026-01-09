import scrapy


class BeltaSpider(scrapy.Spider):
    name = "Belta"
    allowed_domains = ["belta.by"]
    start_urls = ["https://belta.by/all_news/"]

    def parse(self, response):
        pass
