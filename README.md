# Puppy DB Tool Desktop

Linux-first desktop database manager with:

- Go backend (connection/storage/database services)
- Wails desktop shell
- Modern web frontend (`frontend/`) for the UI

## Current UI

- Startup screen is a modern **Database Connections Dashboard**
- Search + pagination + connection status
- Add/edit/delete/test/open connection from modal and row actions
- Dark visual style close to provided design references

## Database Support

- PostgreSQL
- MySQL
- MongoDB
- Redis
- Direct connection or SSH tunnel

## Encrypted Local Storage

- Connections/snippets/history are stored in:
  - `~/.puppydb/store.enc`

## Run (Linux)

Prerequisites:

- Go 1.22+
- Node.js 20+ and npm
- Wails CLI (`wails`)

Install and run in dev mode:

```bash
./run.sh
```

Build desktop binary:

```bash
./run.sh build
```

Build Windows `.exe` (from Linux):

```bash
./run.sh build-win
```

Output:
- `build/bin/puppy-db-tool.exe`

Build Linux installer package (`.deb`):

```bash
./package-linux.sh 0.1.0
sudo dpkg -i dist/puppy-db-tool_0.1.0_amd64.deb
```

## Notes

- The previous Fyne UI code still exists under `internal/ui` as legacy implementation.
- Active desktop entry now uses Wails (`main.go`, `app.go`, `frontend/`).
