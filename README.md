# ViralVibes - YouTube Trends, Decoded

A powerful web application that analyzes YouTube playlists to uncover viral trends and engagement patterns. Built with FastHTML and MonsterUI for a modern, responsive interface.

[![Deploy with Vercel](https://vercel.com/button)](https://vercel.com/new/clone?repository-url=https://github.com/vercel/examples/tree/main/framework-boilerplates/fasthtml&template=fasthtml)

## Features

- 🔍 Analyze any YouTube playlist for viral trends
- 📊 View detailed engagement metrics and statistics
- 🎯 Calculate engagement rates and performance indicators
- 📱 Responsive design with modern UI components
- 🔒 Secure newsletter signup with Supabase integration

## Tech Stack

- **Frontend**: FastHTML, MonsterUI, TailwindCSS
- **Backend**: Python, FastHTML
- **Database**: Supabase
- **Deployment**: Vercel
- **YouTube Data**: yt-dlp

## Getting Started

1. Clone the repository:
```bash
git clone https://github.com/yourusername/viralvibes.git
cd viralvibes
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. Set up environment variables:
```bash
cp .env.example .env
# Edit .env with your Supabase credentials
```

4. Run locally:
```bash
python main.py
```
The development server will start at http://0.0.0.0:5001

## Architecture

The application follows a modern serverless architecture:

![App Architecture](static/diagram.png)

### Key Components:

1. **Frontend Layer**
   - FastHTML for server-side rendering
   - MonsterUI components for modern UI
   - Responsive design with TailwindCSS

2. **Backend Layer**
   - Python-based API endpoints
   - YouTube data processing with yt-dlp
   - Supabase integration for data storage

3. **Data Layer**
   - Supabase for user data and analytics
   - Real-time data processing
   - Secure data storage

## Deployment

Deploy to Vercel with one click using the button above, or use the CLI:

```bash
npm install -g vercel
vercel --prod
```

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Acknowledgments

- [FastHTML](https://fastht.ml/) for the web framework
- [MonsterUI](https://monsterui.dev/) for UI components
- [Supabase](https://supabase.io/) for backend services
- [Vercel](https://vercel.com) for deployment

# App Design

See the architecture diagram below:

![App Architecture](static/diagram.png)

