import re

import pandas as pd


def convert_duration_to_minutes(duration):
    if 'h' in duration and 'm' in duration:
        hours, minutes = duration.split('h')
        hours = int(hours.strip())
        minutes = int(minutes.strip().replace('m', ''))
        total_minutes = hours * 60 + minutes
        return total_minutes
    elif 'h' in duration:
        hours = int(duration.strip().replace('h', ''))
        return hours * 60
    elif 'm' in duration:
        minutes = int(duration.strip().replace('m', ''))
        return minutes
    else:
        return 0


def convert_datetime_to_minutes(date_time):
    if isinstance(date_time, pd.Timestamp):
        return date_time.hour * 60 + date_time.minute
    elif isinstance(date_time, str):
        match = re.match(r'(\d{2}):(\d{2})', date_time)
        if match:
            return int(match.group(1)) * 60 + int(match.group(2))
        return None


def convert_total_stops(total):
    if total == "non-stop":
        return 0
    else:
        sp = total.split()
        return int(sp[0])


def convert_to_minutes(time):
    if isinstance(time, pd.Timestamp):  # Verifica si es un Timestamp
        time = time.strftime("%H:%M")
    hours, minutes = map(int, time.split(":"))
    return minutes + hours * 60