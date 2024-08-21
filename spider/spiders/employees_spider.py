import scrapy
from bs4 import BeautifulSoup
from scrapy.linkextractors import LinkExtractor
import psycopg2
from scrapy.utils.project import get_project_settings
import re

class EmployeesSpider(scrapy.Spider):
    name = "employees_spider"
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
        self.cursor.execute("SELECT id, links FROM departments")  # Замените departments на имя вашей таблицы
        links = self.cursor.fetchall()
        for department_id, link in links:
            yield scrapy.Request(link, callback=self.parse, meta={'department_id': department_id})

    def parse(self, response):
        # Ищем все ссылки в элементе с классом 'main-link'
        soup = BeautifulSoup(response.text, 'html.parser')
        main_links = soup.find_all('a', class_='main-link')

        for link in main_links:
            href = link.get('href', '')
            if href.startswith('/about/faculties/'):
                full_url = response.urljoin(href)
                yield scrapy.Request(full_url, callback=self.parse_sotrudniki_page, meta=response.meta)
            else:
                self.logger.debug(f'Link does not start with "/about/faculties/": {href}')


    def parse_sotrudniki_page(self, response):
        # Парсим страницу со списком сотрудников
        soup = BeautifulSoup(response.text, 'html.parser')

        # Найти контейнер с классом 'employees'
        employees_container = soup.find(class_='employees')
        
        if not employees_container:
            self.logger.warning(f'No employees container found on {response.url}')
            return

        # Извлекаем все ссылки внутри этого контейнера
        employee_links = employees_container.find_all('a', href=True)
        
        for link in employee_links:
            href = link['href']
            if href.startswith('/about/employee/'):
                full_url = response.urljoin(href)
                yield scrapy.Request(full_url, callback=self.parse_employee_page, meta=response.meta)


    def parse_employee_page(self, response):
        print("ПАРСИНГ страниц сотрудников")
        
        soup = BeautifulSoup(response.text, 'html.parser')

        # Извлечение данных
        title = soup.find(class_='page-header__title').get_text(strip=True) if soup.find(class_='page-header__title') else None

        # Извлечение всех текстов из элементов с классом 'employee__link'
        link_texts = [link.get_text(strip=True) for link in soup.find_all(class_='employee__link')]
        # Извлечение всех текстов из элементов с классом 'employee__subtitle _visually-h4'
        subtitles = [subtitle.get_text(strip=True) for subtitle in soup.find_all(class_='employee__subtitle _visually-h4')]
        # Объединение всех извлеченных текстов в одну строку через пробел
        combined_text = ' '.join(link_texts + subtitles) if link_texts or subtitles else None
        
        achievements_list = [achieve.get_text(strip=True) for achieve in soup.find_all(class_='employee-aside__achievement')]
        degree_set = set()
        title_set = set()
        degree_pattern = re.compile(r'Ученая степень:\s*(.*)')
        title_pattern = re.compile(r'Ученое звание:\s*(.*)')
        for text in achievements_list:
            degree_match = degree_pattern.search(text)
            title_match = title_pattern.search(text)
            
            if degree_match:
                degree_set.add(degree_match.group(1))
            if title_match:
                title_set.add(title_match.group(1))

        # Формирование результирующей строки
        degree_text = ' '.join(degree_set) if degree_set else None
        title_text = ' '.join(title_set) if title_set else None
        degree_str = f"Ученая степень: {degree_text}" if degree_text else None
        title_str = f"Ученое звание: {title_text}" if title_text else None
        
        # Контактная информация (телефон и email)
        contact_info = soup.find_all(class_='employee-aside__contact-link')
        phone = None
        email = None
        for info in contact_info:
            text = info.get_text(strip=True)
            if "@" in text:
                email = text
            else:
                phone = text

        # Информация о SPIN, ORCID и т.д.
        spin = orcid = researcher_id = scopus_author_id = None
        info_list = soup.find_all(class_='employee-aside__info')
        for info in info_list:
            text = info.get_text(strip=True)
            if "SPIN" in text:
                spin = text
            elif "ORCID" in text:
                orcid = text
            elif "ResearcherID" in text:
                researcher_id = text
            elif "Scopus AuthorID" in text:
                scopus_author_id = text

        # Сохранение данных в БД
        department_id = response.meta['department_id']
        self.save_to_db(
            title, combined_text, degree_str, title_str, phone, email, spin, orcid, researcher_id, scopus_author_id, response.url, department_id
        )

    def save_to_db(self, title, combined_text, degree_str, title_str, phone, email, spin, orcid, researcher_id, scopus_author_id, link, department_id):
        try:
            self.cursor.execute('''
                INSERT INTO employees (departments_id, name, status, achievement_academic_degree, achievement_academic_title, phone_number, email, spin, orcid, researcherid, scopus_authorid, link) 
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ''', (department_id, title, combined_text, degree_str, title_str, phone, email, spin, orcid, researcher_id, scopus_author_id, link))
            self.conn.commit()
        except psycopg2.Error as e:
            self.logger.error(f'Error saving data to DB: {e}')  

    def close_spider(self, spider):
        self.cursor.close()
        self.conn.close()
