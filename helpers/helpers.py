import re


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


def convert_datetime_to_minutes(date_time_str):

    match = re.match(r'(\d{2}):(\d{2})', date_time_str)
    if match:

        hour = int(match.group(1))
        minute = int(match.group(2))

        minutes = hour * 60 + minute
        return minutes
    else:
        return None


def convert_Total_Stops(total):
    if total == "non-stop":
        return 0
    else:
        sp = total.split()
        return int(sp[0])
