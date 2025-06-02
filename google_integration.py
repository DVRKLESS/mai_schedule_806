import os
import json
import sqlite3
from pathlib import Path
from datetime import datetime
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import Flow
from googleapiclient.discovery import build



from flask import redirect, url_for, session
SCOPES = ["https://www.googleapis.com/auth/calendar"]
API_SERVICE_NAME = "calendar"
API_VERSION = "v3"
PROJECT_DIR = Path(__file__).parent.resolve()
CLIENT_SECRETS_FILE = PROJECT_DIR / "credentials.json"
DB_PATH = PROJECT_DIR / "schedule.db"

class GoogleCalendarIntegration:
	def __init__(self, app):
		self.app = app
		self.credentials = None
		self.app.secret_key = os.urandom(24)
		self.app.config['SESSION_TYPE'] = 'filesystem'
		self.token_file = Path(__file__).parent / "google_token.json"


	def authorize(self):
		flow = Flow.from_client_secrets_file(
			CLIENT_SECRETS_FILE,
			scopes=SCOPES,
			redirect_uri=url_for('oauth2callback', _external=True))
		authorization_url, state = flow.authorization_url(
			access_type='offline',
			include_granted_scopes='true')
		session['state'] = state
		return authorization_url


	def save_credentials(self, auth_response):
		flow = Flow.from_client_secrets_file(
			CLIENT_SECRETS_FILE,
			scopes=SCOPES,
			state=session['state'],
			redirect_uri=url_for('oauth2callback', _external=True))
		flow.fetch_token(authorization_response=auth_response)
		credentials = flow.credentials
		with open(self.token_file, 'w') as token:
			token.write(credentials.to_json())
		return credentials
	
	
	def get_calendar_service(self):
		if not os.path.exists(self.token_file):
			return None
		with open(self.token_file, 'r') as token:
			creds_data = json.load(token)
			creds = Credentials.from_authorized_user_info(info=creds_data, scopes=SCOPES)
		return build(API_SERVICE_NAME, API_VERSION, credentials=creds)


	def sync_schedule_to_calendar(self, schedule_data=None, calendar_id='primary', group=None):
		service = self.get_calendar_service()
		if not service:
			raise Exception("Not authenticated with Google Calendar")
		with sqlite3.connect(DB_PATH) as conn:
			conn.row_factory = sqlite3.Row
			query = """
			SELECT * FROM schedule 
			WHERE group_name = ?
			ORDER BY date, start_time
			"""
			lessons = conn.execute(query, (group,)).fetchall()
		success_count = 0
		existing_count = 0
		error_count = 0
		for lesson in lessons:
			try:
				event = self._create_event_from_lesson(lesson)
				events_result = service.events().list(
					calendarId=calendar_id,
					q=event['summary'],
					maxResults=1
				).execute()
				events = events_result.get('items', [])
				if not events:
					service.events().insert(
						calendarId=calendar_id,
						body=event
					).execute()
					success_count += 1
				else:
					existing_count += 1
			except Exception:
				error_count += 1
				continue
		return {
			"status": "success",
			"added": success_count,
			"existing": existing_count,
			"errors": error_count
		}


	def _create_event_from_lesson(self, lesson):
		try:
			day, month = map(int, lesson['date'].split('.'))
			year = datetime.now().year
			current_month = datetime.now().month
			if current_month >= 9 and month < 9:
				year += 1
			date_str = f"{year}-{month:02d}-{day:02d}"
			start_time_str = lesson['start_time']
			end_time_str = lesson['end_time']
			start_datetime = datetime.strptime(f"{date_str} {start_time_str}", "%Y-%m-%d %H:%M")
			end_datetime = datetime.strptime(f"{date_str} {end_time_str}", "%Y-%m-%d %H:%M")
			if end_datetime <= start_datetime:
				raise ValueError(f"Время окончания {end_time_str} должно быть позже времени начала {start_time_str}")
			return {
				'summary': f"{lesson['subject']} ({lesson['group_name']})",
				'location': lesson['classroom'],
				'description': f"Тип: {lesson['type']}\nГруппа: {lesson['group_name']}",
				'start': {
					'dateTime': start_datetime.isoformat(),
					'timeZone': 'Europe/Moscow',
				},
				'end': {
					'dateTime': end_datetime.isoformat(),
					'timeZone': 'Europe/Moscow',
				},
				'reminders': {
					'useDefault': True,
				},
			}
		except Exception as e:
			print(f"Ошибка создания события для занятия {lesson}: {str(e)}")
			raise


	def _compare_event_with_lesson(self, event, lesson):
		return (
			event.get('summary') == f"{lesson['subject']} ({lesson['group_name']})" and
			event.get('location') == lesson['classroom'] and
			event.get('description', '').startswith(f"Преподаватель: {lesson.get('teacher', '')}")
		)
