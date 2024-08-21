import scrapy
from bs4 import BeautifulSoup
from scrapy.linkextractors import LinkExtractor
import psycopg2
from scrapy.utils.project import get_project_settings
import re

class EmployeesSpider(scrapy.Spider):
    name = "data_spider"
    allowed_domains = [""]

    def __init__(self, *args, **kwargs):
        super(EmployeesSpider, self).__init__(*args, **kwargs)
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

    def start_requests(self):
        # Извлекаем ссылки и ID кафедр из базы данных
        self.cursor.execute("SELECT employees_id, link FROM employees")  # Замените departments на имя вашей таблицы
        links = self.cursor.fetchall()
        for employees_id, link in links:
            yield scrapy.Request(link, callback=self.parse, meta={'employees_id': employees_id})
            
    def parse(self, response):
        # Ищем все секции с классом 'section'
        soup = BeautifulSoup(response.text, 'html.parser')
        sections = soup.find_all('section', class_='section')
        
        # Инициализируем переменные для хранения контента
        publications_text = ''
        editorial_activity_text = ''
        education_text = ''
        
        # Обрабатываем каждую секцию
        for section in sections:
            header = section.find('div', class_='section__header')
            if header:
                header_text = header.get_text(strip=True)
                content_div = section.find('div', class_='section__content')
                if content_div:
                    content_text = self.clean_text(content_div.get_text(strip=True))
                    if header_text == 'Публикации':
                        publications_text = content_text
                    elif header_text == 'Издательская деятельность':
                        editorial_activity_text = content_text
                    elif header_text == 'Образование':
                        education_text = content_text
        
        # Сохраняем в базу данных
        employees_id = response.meta.get('employees_id')
        self.save_to_db(employees_id, publications_text, editorial_activity_text, education_text)
        
    def clean_text(self, text):
        # Удаляем лишние пробелы и оставляем по одному пробелу между словами
        return re.sub(r'\s+', ' ', text).strip()
    
    
    def save_to_db(self, employees_id, publications_text, editorial_activity_text, education_text):
        try:
            self.cursor.execute('''
                INSERT INTO data_empl (employees_id, publications, editorial_activity, education) 
                VALUES (%s, %s, %s, %s)
            ''', (employees_id, publications_text, editorial_activity_text, education_text))
            self.conn.commit()
        except psycopg2.Error as e:
            self.logger.error(f'Error saving data to DB: {e}')  

    def close_spider(self, spider):
        self.cursor.close()
        self.conn.close()



