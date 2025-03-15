from fasthtml.common import *
import yt_dlp
import pandas as pd
#import squarify
#import matplotlib.pyplot as plt
import io
import base64

app, rt = fast_app(hdrs=(picolink))

def get_playlist_videos(playlist_url):
    """Extracts video data from a YouTube playlist."""
    ydl_opts = {
        'quiet': True,
        'extract_flat': True,
        'force_generic_extractor': True
    }
    
    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
        playlist_info = ydl.extract_info(playlist_url, download=False)

        if 'entries' in playlist_info:
            videos = playlist_info['entries']
            data = [{
                "Rank": rank, 
                "Title": video.get('title', 'N/A'), 
                "Views": video.get('view_count', 0),
                "Uploader": video.get('uploader', 'N/A'),
                "Channel ID": video.get('channel_id', 'N/A')
            } for rank, video in enumerate(videos, start=1)]
            
            return pd.DataFrame(data)
    return pd.DataFrame()

@rt("/")
def get():
    return (
        Socials(
            title="Vercel + FastHTML",
            site_name="Vercel",
            description="A demo of Vercel and FastHTML integration",
            image="https://vercel.fyi/fasthtml-og",
            url="https://fasthtml-template.vercel.app",
            twitter_site="@vercel",
        ),
        Container(
            Card(
                Group(
                    P(
                        "FastHTML is a new next-generation web framework for fast, scalable web applications with minimal, compact code. It builds on top of popular foundations like ASGI and HTMX. You can now deploy FastHTML with Vercel CLI or by pushing new changes to your git repository.",
                    ),
                ),
                header=(Titled("FastHTML + Vercel")),
                footer=(
                    P(
                        A(
                            "Deploy your own",
                            href="https://vercel.com/templates/python/fasthtml-python-boilerplate",
                        ),
                        " or ",
                        A("learn more", href="https://docs.fastht.ml/"),
                        "about FastHTML.",
                    )
                ),
            ),
        ),
    )


serve()
