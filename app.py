from flask import Flask, render_template, jsonify, request, session, redirect, url_for
from google_integration import GoogleCalendarIntegration
import sqlite3
import os
from functools import lru_cache
from datetime import datetime
import time
import traceback


os.environ["OAUTHLIB_INSECURE_TRANSPORT"] = "1"
app = Flask(__name__)
google_calendar = GoogleCalendarIntegration(app)


DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "schedule.db")
CACHE_TTL = 300


cache = {
    "groups": {"data": None, "timestamp": 0},
    "current_week": {"data": None, "timestamp": 0},
}


def get_db_connection():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def get_cached_data(key):
    if time.time() - cache[key]["timestamp"] < CACHE_TTL:
        return cache[key]["data"]
    return None


def set_cached_data(key, data):
    cache[key] = {"data": data, "timestamp": time.time()}


@app.route("/")
def index():
    return render_template("index.html")


@app.route("/api/groups")
def get_groups():
    cached_data = get_cached_data("groups")
    if cached_data is not None:
        return jsonify(cached_data)

    with get_db_connection() as conn:
        groups = [
            row["group_name"]
            for row in conn.execute(
                "SELECT DISTINCT group_name FROM schedule"
            ).fetchall()
        ]
        set_cached_data("groups", groups)
        return jsonify(groups)


@app.route("/api/current_week")
def get_current_week():
    cached_data = get_cached_data("current_week")
    if cached_data is not None:
        return jsonify(cached_data)

    today = datetime.now().strftime("%d.%m")
    with get_db_connection() as conn:
        week = conn.execute(
            "SELECT DISTINCT week_number FROM schedule WHERE date = ? LIMIT 1", (today,)
        ).fetchone()
        if week:
            result = {"week": week["week_number"]}
        else:
            week = conn.execute(
                """
				SELECT week_number FROM schedule 
				WHERE date >= ? 
				ORDER BY date 
				LIMIT 1
			""",
                (today,),
            ).fetchone()
            result = {"week": week["week_number"] if week else 1}

        set_cached_data("current_week", result)
        return jsonify(result)


@lru_cache(maxsize=32)
def get_group_schedule(group):
    with get_db_connection() as conn:
        return [
            dict(row)
            for row in conn.execute(
                """
			SELECT * FROM schedule 
			WHERE group_name = ?
			ORDER BY week_number, date, start_time
		""",
                (group,),
            ).fetchall()
        ]


@app.route("/api/schedule")
def get_schedule():
    group = request.args.get("group")
    if not group:
        return jsonify({"error": "Не указана группа"}), 400

    with get_db_connection() as conn:
        schedule = conn.execute(
            """
			SELECT 
				week_number,
				day_name,
				date,
				start_time,
				end_time,
				subject,
				classroom,
				type
			FROM schedule 
			WHERE group_name = ?
			ORDER BY week_number, date, start_time
		""",
            (group,),
        ).fetchall()

        return jsonify([dict(row) for row in schedule])


@app.route("/api/occupancy")
def get_occupancy():
    try:
        date = request.args.get("date")
        start_time = request.args.get("start")
        end_time = request.args.get("end")

        if not date or not start_time or not end_time:
            return (
                jsonify({"error": "Необходимо указать дату и временной диапазон"}),
                400,
            )

        date_obj = datetime.strptime(date, "%Y-%m-%d")
        db_date = date_obj.strftime("%d.%m")

        with get_db_connection() as conn:
            lessons = []
            unique_audiences = set()

            for row in conn.execute(
                """
				SELECT group_name, classroom, start_time, end_time 
				FROM schedule 
				WHERE date = ? AND (
					(start_time < ? AND end_time > ?) OR
					(start_time BETWEEN ? AND ?) OR
					(end_time BETWEEN ? AND ?)
				)
				ORDER BY start_time
			""",
                (
                    db_date,
                    end_time,
                    start_time,
                    start_time,
                    end_time,
                    start_time,
                    end_time,
                ),
            ):
                classroom = row["classroom"].strip()
                if classroom == "--каф.":
                    classroom = "806каф."

                lessons.append(
                    {
                        "group": row["group_name"],
                        "classroom": classroom,
                        "time": f"{row['start_time']}-{row['end_time']}",
                    }
                )
                unique_audiences.add(classroom)

            return jsonify(
                {
                    "occupied_count": len(unique_audiences),
                    "total_count": 9,
                    "lessons": lessons,
                    "debug_time": datetime.now().strftime("%H:%M:%S"),
                }
            )

    except Exception as e:
        tb = traceback.format_exc()
        app.logger.error(f"Exception: {e}\n{tb}")
        return jsonify({"error": str(e), "traceback": tb}), 500


@app.route("/api/sync/calendar", methods=["POST"])
def sync_to_calendar():
    if not google_calendar.get_calendar_service():
        return (
            jsonify(
                {
                    "status": "auth_required",
                    "auth_url": url_for("authorize", _external=True),
                }
            ),
            401,
        )

    try:
        group = request.args.get("group")
        if not group:
            return jsonify({"error": "Не указана группа"}), 400

        result = google_calendar.sync_schedule_to_calendar(group=group)
        return jsonify(result)

    except Exception as e:
        tb = traceback.format_exc()
        app.logger.error(f"Exception: {e}\n{tb}")
        return jsonify({"status": "error", "message": str(e), "traceback": tb}), 500


def clear_cache():
    global cache
    cache = {
        "groups": {"data": None, "timestamp": 0},
        "current_week": {"data": None, "timestamp": 0},
    }
    get_group_schedule.cache_clear()


@app.route("/authorize")
def authorize():
    auth_url = google_calendar.authorize()
    return redirect(auth_url)


@app.route("/oauth2callback")
def oauth2callback():
    try:
        google_calendar.save_credentials(request.url)
        return redirect(url_for("index"))
    except Exception as e:
        return f"Ошибка авторизации: {str(e)}", 400


@app.route("/api/subjects")
def get_subjects():
    with get_db_connection() as conn:
        try:
            subjects = [
                row["subject_name"]
                for row in conn.execute(
                    "SELECT subject_name FROM subjects ORDER BY subject_name"
                )
            ]
            if not subjects:
                raise sqlite3.OperationalError
        except sqlite3.OperationalError:
            subjects = [
                row["subject"]
                for row in conn.execute(
                    "SELECT DISTINCT subject FROM schedule ORDER BY subject"
                )
            ]
        return jsonify(subjects)


@app.route("/api/subject_schedule")
def get_subject_schedule():
    subject = request.args.get("subject")
    week = request.args.get("week")

    with get_db_connection() as conn:
        schedule = conn.execute(
            """
			SELECT day_name, date, start_time, end_time, classroom, type
			FROM schedule
			WHERE subject = ? AND week_number = ?
			ORDER BY date, start_time
		""",
            (subject, week),
        ).fetchall()
        return jsonify([dict(row) for row in schedule])


if __name__ == "__main__":
    app.run(debug=True, port=5000)
