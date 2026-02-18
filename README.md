# AI Google Cast TV Alarm (Python)

A Python-first app that serves a customizable HTML clock page and casts it to a Google TV/Chromecast.

## Features

- Large full-screen live clock
- Custom time rules for behavior changes
- Demo rule: flash the time red after 8:00 AM
- Sound chime every 5 minutes
- Optional Wake-on-LAN before casting

## Setup

1. Install dependencies:

```powershell
python -m pip install -r requirements.txt
```

2. Create config:

```powershell
Copy-Item config.example.json config.json
```

3. Edit `config.json`:

- Leave `server.public_base_url` as `"auto"` (recommended). The app will detect the host PC IPv4 at runtime.
- Optional override: set `server.public_base_url` manually, e.g. `http://192.168.1.50:8765`, if auto-detection does not match your network route.
- Set `cast.friendly_name` to your Google TV device name (recommended for DHCP networks).
- Optional: set `cast.device_uuid` (stable target ID, also DHCP-friendly).
- Optional: set `cast.ip` only as a fallback hint (you do not need static TV IP).
- Optionally set `cast.wake_on_lan_mac` if your TV supports Wake-on-LAN.
- `cast.stop_app_before_cast=true` forces takeover from a stale/previous cast session.
- `cast.stop_on_exit=true` stops the cast app when this Python process exits.

## Run

```powershell
python app.py --config config.json
```

When the app starts, it:

- Serves the clock page at `/clock`
- Attempts to wake the TV (if configured)
- Casts the clock page to the Google TV

Useful control endpoints:

- `POST /api/cast/recast` starts a recast attempt
- `POST /api/cast/stop` stops the active cast app
- `GET /api/cast/status` returns current cast receiver app/namespace status plus last end reason
- `GET /api/keys` returns recent captured key events
- `POST /api/keys` accepts key debug events from the page
- `GET /api/debug/events` returns recent server-side lifecycle/debug events

Cast session monitoring:

- The server now monitors receiver app status directly (not only key events).
- If the TV switches away from DashCast (for example Home/YouTube buttons), the app marks the cast session as ended automatically.
- Tune polling with `cast.status_poll_seconds` and `cast.status_miss_limit`.
- Some TVs briefly report `app_id=None` during app transitions. Use:
  - `cast.status_switch_confirmations` to require repeated non-DashCast status before ending.
  - `cast.status_startup_grace_seconds` to ignore transient startup jitter.
  - `cast.status_end_on_unavailable` to control whether empty/missing status should end the session (default `false`).
  - `cast.status_unavailable_log_every` to log recurring unavailable-status warnings without ending the session.
- For TVs that keep receiver status empty, page heartbeats are used as the fallback liveness signal:
  - `cast.page_heartbeat_required`
  - `cast.page_heartbeat_timeout_seconds`
  - `cast.page_heartbeat_start_grace_seconds`

TV idle / screensaver prevention:

- Configure `page.anti_idle` to keep the TV awake while the clock is active:
  - `enabled`: master switch
  - `screen_wake_lock`: request browser screen wake lock when supported
  - `audio_keepalive`: run near-silent continuous audio to discourage idle/screensaver
  - `audio_gain`: keepalive gain (very low by default)
  - `ping_interval_ms`: heartbeat interval while active

Process/lifecycle debugging:

- Server logs now include structured `DBG` lines for cast attempts, monitor events, and shutdown paths.
- `CAST_END reason='process-exit'` means the Python process is exiting, not just the cast session.
- Use `GET /api/debug/events?limit=400` to inspect why Flask exited (`flask_run_returned`, `flask_run_exception`, `keyboard-interrupt`, etc.).
- Debug events are also written to `runtime_debug.log` by default (override with `debug.log_file` in config).

On-screen controls / remote keys:

- `Test Chime` button (or `Enter` / `C`)
- `Mute: On/Off` button (or `M`)
- `Stop Cast` button (or `Backspace` / `Escape`) sends `POST /api/cast/stop`
- `Key Debug: On/Off` button shows a live key event HUD

## Customization

Edit `page.rules` in `config.json` to apply CSS classes during time windows.

Example demo rule (included by default):

```json
{
  "name": "flash-red-after-8am",
  "start": "08:00",
  "end": null,
  "class_name": "flash-red"
}
```

This makes the clock flash red from 8:00 AM onward.

You can add more classes/styles in `templates/clock.html` and point rules to those class names.
