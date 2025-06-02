import sqlite3
from pathlib import Path


DB_PATH = Path(__file__).parent / "schedule.db"

def create_subjects_table():
	with sqlite3.connect(DB_PATH) as conn:
		cursor = conn.cursor()
		cursor.execute("""
			CREATE TABLE IF NOT EXISTS subjects (
				id INTEGER PRIMARY KEY,
				subject_name TEXT NOT NULL UNIQUE
			)
		""")
		
		cursor.execute("SELECT DISTINCT subject FROM schedule")
		unique_subjects = [row[0] for row in cursor.fetchall()]
		for subject in unique_subjects:
			cursor.execute("""
				INSERT OR IGNORE INTO subjects (subject_name)
				VALUES (?)
			""", (subject,))
		cursor.execute("SELECT COUNT(*) FROM subjects")
		count = cursor.fetchone()[0]
		print(f"Таблица subjects создана. Добавлено {count} уникальных предметов.")

if __name__ == "__main__":
	print("Начинаю обработку базы данных...")
	create_subjects_table()
	print("Готово!")