import os
import json
import threading
import logging
from queue import Queue
from concurrent.futures import ThreadPoolExecutor, as_completed
from functools import partial
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
from bs4 import BeautifulSoup
from tqdm import tqdm
from webdriver_manager.chrome import ChromeDriverManager
import re
from datetime import datetime
from selenium.webdriver.common.service import Service as SeleniumService
import sqlite3

# Настройки
MAX_WEEKS = 22
PRE_CHECK_WEEKS = 6
MAX_WORKERS = 5
BASE_URL = "https://mai.ru/education/studies/schedule/index.php"
DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "schedule.db")
SeleniumService.LOG_FILE = os.devnull

# Целевые предметы для фильтрации
TARGET_KEYWORDS = [
	'разработка', 'python', 'алгоритмы', 'структуры данных',
	'инструментальные', '3d-моделирование', 'blender',
	'машинное обучение', 'программная инженерия', 'мультимедиа',
	'интеллектуальной поддержки', 'параллельные вычисления',
	'криптографии', 'базы данных', 'анализ больших данных'
]

EXCLUDE_KEYWORDS = ['лекция', 'семинар']

class DriverPool:
	def __init__(self, max_drivers):
		self._pool = Queue(max_drivers)
		self._lock = threading.Lock()
		for _ in range(max_drivers):
			self._pool.put(self._create_driver())

	def _create_driver(self):
		chrome_options = Options()
		chrome_options.add_argument("--headless=new")
		chrome_options.add_argument("--disable-gpu")
		chrome_options.add_argument("--window-size=1920x1080")
		chrome_options.add_argument("--no-sandbox")
		chrome_options.add_argument("--disable-dev-shm-usage")
		chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")
		chrome_options.add_argument("--disable-webgl")
		chrome_options.add_argument("--log-level=3")
		chrome_options.add_argument("--disable-logging")
		chrome_options.add_argument("--silent")
		service = Service(ChromeDriverManager().install())
		return webdriver.Chrome(service=service, options=chrome_options)

	def get_driver(self):
		with self._lock:
			return self._pool.get()

	def release_driver(self, driver):
		with self._lock:
			try:
				_ = driver.title
				self._pool.put(driver)
			except Exception as e:
				logging.warning(f"Драйвер мертв, пересоздаем: {str(e)}")
				self._pool.put(self._create_driver())

DRIVER_POOL = DriverPool(MAX_WORKERS)

def init_driver():
	return DRIVER_POOL.get_driver()

def init_db():
	with sqlite3.connect(DB_PATH) as conn:
		conn.execute("""
			CREATE TABLE IF NOT EXISTS schedule (
				id INTEGER PRIMARY KEY,
				group_name TEXT NOT NULL,
				week_number INTEGER NOT NULL,
				day_name TEXT NOT NULL,
				date TEXT,
				start_time TEXT NOT NULL,
				end_time TEXT NOT NULL,
				subject TEXT NOT NULL,
				classroom TEXT NOT NULL,
				type TEXT NOT NULL,
				UNIQUE(group_name, week_number, day_name, start_time, subject)
			)
		""")

def save_to_db(group_name, week_number, day_data, lesson):
	day_name = day_data['day'].split(',')[0].strip()

	date = None
	if ',' in day_data['day']:
		date_part = day_data['day'].split(',')[1].strip()
		if '.' in date_part:
			date = date_part.split()[0]
		else:
			try:
				day, month = re.search(r'(\d+)\s+([а-я]+)', date_part).groups()
				month_num = {
					'января': '01', 'февраля': '02', 'марта': '03',
					'апреля': '04', 'мая': '05', 'июня': '06',
					'июля': '07', 'августа': '08', 'сентября': '09',
					'октября': '10', 'ноября': '11', 'декабря': '12'
				}.get(month.lower(), '01')
				date = f"{int(day):02d}.{month_num}"
			except:
				pass

	start_time, end_time = lesson['time'].split('–') if lesson.get('time') else (None, None)
	if not start_time or not end_time:
		return
	
	with sqlite3.connect(DB_PATH) as conn:
		conn.execute("""
			INSERT OR IGNORE INTO schedule (
				group_name, week_number, day_name, date,
				start_time, end_time, subject, classroom, type
			) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
		""", (
			group_name,
			week_number,
			day_name,
			date,
			start_time.strip(),
			end_time.strip(),
			lesson['subject'],
			lesson.get('classroom', 'каф. 806'),
			lesson.get('type', '')
		))


def contains_target_subject(subject_text):
	subject_lower = subject_text.lower()
	for excl in EXCLUDE_KEYWORDS:
		if excl in subject_lower:
			return False
	for kw in TARGET_KEYWORDS:
		if re.search(r'\b' + re.escape(kw) + r'\b', subject_lower):
			return True
	return False


def get_group_year_suffix(course, current_date=None):
	if current_date is None:
		current_date = datetime.now()
	year = current_date.year
	month = current_date.month
	end_year = year + 1 if month >= 9 else year
	year_suffix = (end_year % 100) - course
	return f"{year_suffix + 100 if year_suffix < 0 else year_suffix:02d}"


def has_target_subjects(html):
	soup = BeautifulSoup(html, 'html.parser')
	for lesson in soup.find_all('div', class_='mb-4'):
		subject_block = lesson.find('p', class_='fw-semi-bold')
		if subject_block and contains_target_subject(' '.join(subject_block.stripped_strings).lower()):
			return True
	return False


def parse_schedule_html(html):
	soup = BeautifulSoup(html, 'html.parser')
	result = []
	for day in soup.find_all('div', class_='step-content'):
		day_title = day.find('span', class_='step-title').get_text(strip=True) or "Неизвестный день"
		lessons = []
		for lesson in day.find_all('div', class_='mb-4'):
			subject_block = lesson.find('p', class_='fw-semi-bold')
			if not subject_block:
				continue

			badge = subject_block.find('span', class_='badge')
			lesson_type = badge.get_text(strip=True).upper() if badge else ""
			subject_text = subject_block.get_text(" ", strip=True).replace(badge.get_text(strip=True) if badge else "", "").strip()

			if not contains_target_subject(subject_text) or 'ЛР' not in lesson_type:
				continue

			time_text = ""
			classroom = ""
			for detail in lesson.find_all('li', class_='list-inline-item'):
				text = detail.get_text(strip=True)
				if '–' in text:
					time_text = text
				elif detail.find('i', class_='fa-map-marker-alt'):
					classroom = ''.join(detail.find('i').next_siblings).strip()

			lessons.append({
				'subject': subject_text,
				'type': lesson_type,
				'time': time_text,
				'classroom': classroom
			})

		if lessons:
			result.append({'day': day_title, 'lessons': lessons})
	return result


def process_group(course, group_num, level_name, level_code):
	driver = None
	try:
		year_suffix = get_group_year_suffix(course)
		group_name = f"М8О-{course}{group_num:02}{level_code}-{year_suffix}"
		driver = init_driver()
		all_weeks_data = {}

		logging.info(f"Проверка группы {group_name}...")

		# Предварительная проверка
		has_target = False
		for week in range(1, PRE_CHECK_WEEKS + 1):
			url = f"{BASE_URL}?group={group_name}&week={week}"
			try:
				driver.get(url)
				WebDriverWait(driver, 5).until(
					EC.presence_of_element_located((By.CSS_SELECTOR, ".mb-4"))
				)
				if has_target_subjects(driver.page_source):
					has_target = True
					break
			except Exception as e:
				logging.warning(f"Группа {group_name}, неделя {week} - ошибка проверки: {str(e)}")
				continue

		if has_target:
			logging.info(f"Группа {group_name} содержит целевые предметы, парсим все недели...")
			for week in range(1, MAX_WEEKS + 1):
				url = f"{BASE_URL}?group={group_name}&week={week}"
				try:
					driver.get(url)
					WebDriverWait(driver, 5).until(
						EC.presence_of_element_located((By.CSS_SELECTOR, ".mb-4"))
					)
					week_data = parse_schedule_html(driver.page_source)
					if week_data:
						all_weeks_data[f"{week} неделя"] = week_data
						for day in week_data:
							for lesson in day['lessons']:
								save_to_db(group_name, week, day, lesson)
				except Exception as e:
					logging.warning(f"Группа {group_name}, неделя {week} - ошибка парсинга: {str(e)}")
					continue

		return f"✅ Группа {group_name} - обработано {len(all_weeks_data)} недель" if all_weeks_data else f"❌ Группа {group_name} - нет целевых предметов"

	except Exception as e:
		logging.error(f"Критическая ошибка для группы {group_name}: {str(e)}")
		return f"💀 Ошибка: {group_name}"
	finally:
		if driver:
			DRIVER_POOL.release_driver(driver)


def main():
	init_db()
	with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
		tasks = []
		
		education_levels = {
			1: [('Специализированное высшее образование', 'СВ'), ('Базовое высшее образование', 'БВ')],
			2: [('Бакалавриат', 'Б')],
			3: [('Бакалавриат', 'Б')],
			4: [('Бакалавриат', 'Б')]
		}

		for course in range(1, 5):
			for group_num in range(1, 20):
				for level_name, level_code in education_levels[course]:
					tasks.append(
						executor.submit(
							partial(process_group, course, group_num, level_name, level_code)
						)
					)

		for future in tqdm(as_completed(tasks), total=len(tasks), desc="Обработка групп"):
			try:
				result = future.result()
				logging.info(result)
			except Exception as e:
				logging.error(f"Ошибка в задаче: {str(e)}")

if __name__ == "__main__":
	logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s: %(message)s')
	logging.info(f"Запуск парсера. БД будет сохранена в: {DB_PATH}")
	try:
		main()
	finally:
		while not DRIVER_POOL._pool.empty():
			driver = DRIVER_POOL._pool.get()
			driver.quit()
	logging.info("Парсинг завершен!")
