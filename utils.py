def calculate_engagement_rate(view_count, like_count, dislike_count):
    if view_count == 0:
        return 0
    return ((like_count + dislike_count) / view_count) * 100


def format_number(number):
    if number >= 1_000_000_000:
        return f"{number / 1_000_000_000:.1f}B"
    elif number >= 1_000_000:
        return f"{number / 1_000_000:.1f}M"
    elif number >= 1_000:
        return f"{number / 1_000:.1f}K"
    return str(number)


def format_duration(seconds):
    try:
        seconds = float(seconds)
        if seconds <= 0:
            return "N/A"
        minutes, seconds = divmod(int(seconds), 60)
        hours, minutes = divmod(minutes, 60)

        if hours > 0:
            return f"{int(hours)}:{int(minutes):02d}:{int(seconds):02d}"
        else:
            return f"{int(minutes)}:{int(seconds):02d}"
    except Exception:
        return "N/A"
