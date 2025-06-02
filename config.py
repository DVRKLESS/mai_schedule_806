import os
from pathlib import Path

# Базовые настройки
BASE_DIR = Path(__file__).parent

# Настройки Flask
class FlaskConfig:
	DEBUG = True
	SECRET_KEY = 'your-secret-key-here'
	TEMPLATES_AUTO_RELOAD = True

# Настройки базы данных
DATABASE_CONFIG = {
	'path': BASE_DIR / 'schedule.db',
	'timeout': 30
}

# Настройки парсера
PARSER_CONFIG = {
	'target_keywords': [
		'разработка', 'python', 'алгоритмы', 'структуры данных',
		'инструментальные', '3d-моделирование', 'blender'
	],
	'exclude_keywords': ['лекция', 'семинар'],
	'max_workers': 5
}