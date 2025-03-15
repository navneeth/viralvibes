from fasthtml.common import *
import yt_dlp
import pandas as pd
import squarify
import matplotlib.pyplot as plt
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

def create_plot(df, field="Uploader", chart_type="treemap"):
    """Generates treemap or bar chart as a base64 image."""
    df_filtered = df.groupby(field).sum().reset_index()
    df_filtered = df_filtered[df_filtered['Views'] > 1_000_000_000]  # Filter out entries <1B views
    
    plt.figure(figsize=(12, 8))
    
    if chart_type == "treemap":
        labels = df_filtered.apply(lambda x: f"{x[field]}\n{x['Views'] / 1e9:.2f}B views", axis=1)
        squarify.plot(sizes=df_filtered["Views"], label=labels, alpha=0.8, text_kwargs={'fontsize': 10})
        plt.title(f"Treemap of {field}", fontsize=16, fontweight='bold')
    else:
        df_filtered = df_filtered.sort_values("Views", ascending=False)[:20]
        plt.barh(df_filtered[field], df_filtered["Views"] / 1e9, color='skyblue')
        plt.xlabel("Views (Billions)")
        plt.ylabel(field)
        plt.title(f"Top 20 {field}s by Views", fontsize=16, fontweight='bold')
        plt.gca().invert_yaxis()
    
    plt.axis('off' if chart_type == "treemap" else 'on')
    
    buf = io.BytesIO()
    plt.savefig(buf, format="png")
    buf.seek(0)
    return f"data:image/png;base64,{base64.b64encode(buf.read()).decode()}"


@rt("/", methods=["GET", "POST"])
def get(req):
    playlist_url = "https://www.youtube.com/playlist?list=PLirAqAtl_h2r5g8xGajEwdXd3x1sZh8hC"
    chart_image = None

    if req.method == "POST":  # Fix: Use req.method to check the HTTP method
        playlist_url = req.form.get("playlist_url", playlist_url)  # Fix: Use req.form to access form data
        df = get_playlist_videos(playlist_url)
        if not df.empty:
            chart_image = create_plot(df)

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
                    Form(
                        Input(
                            name="playlist_url",
                            value=playlist_url,
                            placeholder="Enter YouTube playlist URL",
                        ),
                        Button("Generate Chart", type="submit"),
                        method="POST",
                    ),
                    Img(src=chart_image) if chart_image else None,
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
