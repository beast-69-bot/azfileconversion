# azfileconversion

File Conversion Bot (Telegram)

Converts documents that are actually media back into media messages before re-sending.
- Video documents -> send_video
- Audio documents -> send_audio
- Image documents -> send_photo
- Other documents -> re-send as document (same filename)
- Unknown/unsupported types -> ignored

Setup
1) Create a Telegram bot and get the bot token.
2) Get API_ID and API_HASH from https://my.telegram.org.
3) Create .env from .env.example and fill values.
4) Install deps: pip install -r requirements.txt
5) Run: python main.py

Notes
- Progress is shown by editing a status message during download/upload.
- Duplicate file_unique_id messages are ignored in-memory.
- Temp files are stored in a temporary directory and removed automatically.