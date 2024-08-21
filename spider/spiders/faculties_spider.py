import scrapy
from bs4 import BeautifulSoup
from scrapy.linkextractors import LinkExtractor
import psycopg2
from scrapy.utils.project import get_project_settings

class FacultiesSpider(scrapy.Spider):
    name = "faculties_spider"
    allowed_domains = ["ssmu.ru"]
    start_urls = [
            "https"
    ]

    def __init__(self, *args, **kwargs):
        super(FacultiesSpider, self).__init__(*args, **kwargs)
        # Получение настроек из settings.py
        settings = get_project_settings()
        self.db_settings = {
            'dbname': settings.get('DB_NAME'),
            'user': settings.get('DB_USER'),
            'password': settings.get('DB_PASSWORD'),
            'host': settings.get('DB_HOST'),
            'port': settings.get('DB_PORT')
        }
        self.conn = psycopg2.connect(**self.db_settings)
        self.cursor = self.conn.cursor()

    def parse(self, response):
        # Извлечение всех ссылок, которые начинаются с "http_allowed"
        le = LinkExtractor(allow=r'http_allowed')
        links = le.extract_links(response)
        
        for link in links:
            if link.url in []:
                continue
            if any(tab in link.url for tab in ['?tab=laboratories', '?tab=faculties', '?tab=departments']):
                continue 

            
            yield scrapy.Request(link.url, callback=self.parse_faculty_page)

    def parse_faculty_page(self, response):
        # Используем BeautifulSoup для извлечения данных
        soup = BeautifulSoup(response.text, 'html.parser')

        # Извлекаем заголовок
        title = soup.find(class_='page-header__title').get_text(strip=True)
        
        # Извлекаем контент
        content = soup.find(class_='page-layout__content text-content').get_text(strip=True)

        # Записываем данные в базу данных
        self.save_to_db(title, content, response.url)

    def save_to_db(self, name, data, link):
        # Сохранение данных в базу данных
        self.cursor.execute('''
            INSERT INTO departments (name, data, links) VALUES (%s, %s, %s)
        ''', (name, data, link))
        self.conn.commit()

    def close_spider(self, spider):
        # Закрываем соединение с базой данных
        self.cursor.close()
        self.conn.close()
