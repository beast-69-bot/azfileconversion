# azfileconversion

Telegram-to-Web direct streaming system with zero server storage for media.

## Project Structure
- `app/config.py` - settings and env loader
- `app/store.py` - token store (in-memory or Redis)
- `app/bot.py` - Pyrogram bot that listens to channel media and generates stream links
- `app/api.py` - FastAPI server that streams from Telegram without saving to disk
- `app/templates/` - (reserved)
- `Procfile` - Heroku process definitions

## How It Works (High Level)
1. Bot receives a media/file in a channel and extracts `file_id` + `file_unique_id`.
2. Generates a secure token and stores token -> file_id mapping (memory or Redis).
3. Replies with `BASE_URL/player/{token}` or `BASE_URL/stream/{token}`.
4. FastAPI `/stream/{token}` fetches from Telegram and streams to the browser in chunks (RAM only).

## Environment
Create `.env` from `.env.example`:
- `API_ID`, `API_HASH`, `BOT_TOKEN`
- `BASE_URL` (your domain)
- `REDIS_URL` (recommended if bot & API are separate processes)
- `TOKEN_TTL_SECONDS`
- `CHUNK_SIZE` (256KB–1MB)

## Run Locally
```bash
pip install -r requirements.txt
python -m app.bot
uvicorn app.api:app --reload
```

## Notes
- If you run bot and API as separate processes, use `REDIS_URL` so the token mapping is shared.
- No media is saved to disk. Only in-memory chunks are streamed.
- Range requests are supported for seeking in video players.

## Chunk-Based Streaming Explanation
The server reads a small chunk from Telegram (e.g., 512KB) and immediately yields it to the client using an async generator. This keeps memory usage low and avoids writing any files to disk. When the client asks for a byte range (HTTP Range), the server uses that range to request only that segment from Telegram and streams it back with proper `Content-Range` headers, enabling video seeking.