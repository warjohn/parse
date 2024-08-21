import scrapy
from scrapy.linkextractors import LinkExtractor
from scrapy.crawler import CrawlerProcess
import csv
from scrapy import signals
from scrapy.signalmanager import dispatcher
from urllib.parse import urlparse

class AllLinksSpider(scrapy.Spider):
    name = "all_links_spider"
    
    # Начальный URL и домен
    start_urls = ['']
    allowed_domains = ['']
    base_domain = ""
    
    def __init__(self, *args, **kwargs):
        super(AllLinksSpider, self).__init__(*args, **kwargs)
        self.seen_links = set()  # Множество для хранения уникальных ссылок
        dispatcher.connect(self.spider_closed, signal=signals.spider_closed)

    def parse(self, response):
        # Используем LinkExtractor для извлечения всех ссылок
        link_extractor = LinkExtractor(allow_domains=self.allowed_domains)
        links = link_extractor.extract_links(response)

        for link in links:
            parsed_url = urlparse(link.url)
            if parsed_url.netloc == self.base_domain and link.url not in self.seen_links:
                self.seen_links.add(link.url)
                yield {'url': link.url}

                # Переходим по каждой найденной ссылке, чтобы найти ссылки на следующих страницах
                yield scrapy.Request(link.url, callback=self.parse)

    def spider_closed(self, spider):
        try:
            with open('urls.csv', 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.writer(csvfile)
                writer.writerow(['url'])  # Заголовок столбца
                for link in sorted(self.seen_links):
                    writer.writerow([link])
        except Exception as e:
            self.logger.error(f"Ошибка при сохранении данных: {e}")

if __name__ == "__main__":
    process = CrawlerProcess()
    process.crawl(AllLinksSpider)
    process.start()



