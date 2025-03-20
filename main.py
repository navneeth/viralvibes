from fasthtml.common import *
from dataclasses import dataclass
from urllib.parse import urlparse, parse_qs


app, rt = fast_app()

# Most Viewed Youtube Videos of all time
# https://www.youtube.com/playlist?list=PLirAqAtl_h2r5g8xGajEwdXd3x1sZh8hC

            
@dataclass
class YoutubePlaylist:
    playlist_url: str

def validate_youtube_playlist(playlist: YoutubePlaylist):
    errors = []
    try:
        parsed_url = urlparse(playlist.playlist_url)
        if parsed_url.netloc != "www.youtube.com" and parsed_url.netloc != "youtube.com":
            errors.append("Invalid YouTube URL: Domain is not youtube.com")
            return errors

        if parsed_url.path != "/playlist":
            errors.append("Invalid YouTube URL: Not a playlist URL")
            return errors

        query_params = parse_qs(parsed_url.query)
        if "list" not in query_params:
            errors.append("Invalid YouTube URL: Missing playlist ID")
            return errors

        playlist_id = query_params["list"][0]

        if not playlist_id:
            errors.append("Invalid YouTube URL: Empty playlist ID")
            return errors

    except ValueError:
        errors.append("Invalid URL format")
        return errors

    return errors

@rt("/")
def get():
    return Titled("Youtube Playlist Validator",
                  Form(Input(type="text", name="playlist_url", placeholder="Youtube Playlist URL"),
                       Button("Validate", type="submit"),
                       hx_post="/validate",
                       hx_target="#result"
                       ),
                       Div(id="result")
                       )

@rt("/validate")
def validate(playlist: YoutubePlaylist):
    errors = validate_youtube_playlist(playlist)
    if errors:
        return Div(Ul(*[Li(error) for error in errors]), id="result", style="color: red;")
    return Div("Valid YouTube Playlist URL", id="result", style="color: green;")


serve()