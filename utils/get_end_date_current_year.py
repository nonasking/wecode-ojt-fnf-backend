from datetime import date, datetime, timedelta

from utils.get_last_sunday import get_last_sunday

def get_end_date_current_year():
    end_date_current_year = get_last_sunday((date.today() - timedelta(weeks=1)).strftime("%Y-%m-%d"))

    return end_date_current_year