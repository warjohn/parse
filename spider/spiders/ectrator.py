import scrapy
import networkx as nx
import csv
from scrapy.exporters import JsonLinesItemExporter
from scrapy.http import HtmlResponse
from urllib.parse import urlparse, urljoin

class DataSpider(scrapy.Spider):
    name = "data_spider"
    start_urls = []

    def __init__(self, *args, **kwargs):
        super(DataSpider, self).__init__(*args, **kwargs)
        self.file = open('output.jsonl', 'wb')
        self.exporter = JsonLinesItemExporter(self.file, encoding='utf-8')
        self.exporter.start_exporting()
        self.G = nx.DiGraph()
        
        # Чтение URL из CSV
        self.load_start_urls()

    def __del__(self):
        self.exporter.finish_exporting()
        self.file.close()

    def load_start_urls(self):
        """Загружает URL из CSV файла."""
        with open("urls_main_domain.csv", "r") as file:
            reader = csv.DictReader(file)
            for row in reader:
                self.start_urls.append(row['url'])

    def start_requests(self):
        """Создает запросы для всех начальных URL."""
        for url in self.start_urls:
            yield scrapy.Request(url=url, callback=self.parse, errback=self.handle_error)

    def parse(self, response):
        # Проверка кода состояния ответа
        if response.status != 200:
            self.logger.error(f"Failed to retrieve {response.url} with status code {response.status}")
            return

        # Извлечение заголовка страницы
        title = response.xpath("//head/title/text()").get()
        if not title:
            title = "No title found"

        # Извлечение HTML-контента внутри <main> внутри <body>
        html_content = response.xpath("//body//main").get()
        if not html_content:
            self.logger.error(f"No <main> content found on {response.url}")
            return

        # Обработка HTML-контента
        content = self.process_html(html_content, response.url)

        # Сохранение данных в формате JSON Lines
        self.exporter.export_item({
            'main_url': response.url,
            'title': title,
            'paths_urls': content['links'],
            'text': content['text']
        })

    def handle_error(self, failure):
        """Обрабатывает ошибки при запросе."""
        self.logger.error(f"Request failed: {failure}")

    def process_html(self, html, base_url):
        # Создаем HTML-ответ для парсинга
        response = HtmlResponse(url='', body=html, encoding='utf-8')

        # Получение текстов и ссылок
        texts = response.xpath("//text()").getall()
        links = response.xpath("//a/@href").getall()

        # Создаем граф и добавляем связи
        for link in links:
            full_url = urljoin(base_url, link)
            base_path = self.get_base_path(full_url)
            if base_path != full_url:
                self.G.add_edge(base_path, full_url)

        # Форматируем связи графа в нужный формат
        path_links = [f"{edge[0]} -> {edge[1]}" for edge in self.G.edges]

        # Форматируем текст с ссылками
        result = []
        for link in links:
            full_url = urljoin(base_url, link)
            text = response.xpath(f"//a[contains(@href, '{link}')]/text()").get()
            if text:
                result.append(f"{text} ({full_url})")

        # Добавляем текстовые узлы, исключая те, которые уже были включены в ссылки
        for text in texts:
            if text.strip() and text.strip() not in result:
                result.append(text.strip())

        # Объединение частей текста и удаление лишних пробелов
        clean_text = ' '.join(result)
        clean_text = ' '.join(clean_text.split())

        return {
            'links': path_links,
            'text': clean_text
        }

    def get_base_path(self, url):
        parsed_url = urlparse(url)
        path_segments = parsed_url.path.rstrip('/').split('/')
        return parsed_url.scheme + "://" + parsed_url.netloc + "/".join(path_segments[:-1]) + '/'
