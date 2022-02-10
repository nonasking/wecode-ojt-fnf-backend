from datetime import datetime, timedelta

def get_last_sunday(date):
    date = datetime.strptime(date, "%Y-%m-%d")
    day_of_the_week = date.weekday()

    day_of_sunday = 6
    last_sunday = date + timedelta(days=(day_of_sunday-day_of_the_week))
    last_sunday = last_sunday.strftime("%Y-%m-%d")

    return last_sunday

if __name__ == "__main__":
    print(get_last_sunday("2022-01-31"))
