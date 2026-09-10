# 🏈 Fantasy Scores — live Sleeper scoreboard

A single-file, phone-first web app that shows your **live fantasy football
matchups** across all of your Sleeper leagues at once. No build step, no
backend, no login — it configures itself from your Sleeper username and talks
to Sleeper's public API straight from the browser.

## Features

- **All your leagues in one view** — one card per league, your matchup up top.
- **Live scores** — auto-refreshes every 15/30/60s (configurable), pausing
  automatically when the tab is in the background to save battery and data.
- **Leader + margin** — the team in front is highlighted, with the current
  margin ("up 15.4") under each matchup.
- **Tap "Lineups"** for a live, position-by-position head-to-head: each
  starter's live points, team/position, and the top scorer highlighted.
- **Records**, team names, and avatars pulled straight from your leagues.
- Works offline-tolerant: failed leagues show a note and retry on the next tick.

## Usage

Open the page and it loads the leagues for the default username. Tap the ⚙️
gear to change:

- **Sleeper username** (defaults to `chasecovert`)
- **Season** (blank = follow the current NFL season automatically)
- **Auto-refresh** interval

Settings persist in `localStorage`. On iOS/Android you can **Add to Home
Screen** for an app-like, full-screen experience.

## How it works

All data comes from the [Sleeper public API](https://docs.sleeper.com/)
(read-only, no key required):

| Data | Endpoint |
| --- | --- |
| Current week / season | `/v1/state/nfl` |
| Your user id | `/v1/user/{username}` |
| Your leagues | `/v1/user/{user_id}/leagues/nfl/{season}` |
| Teams & records | `/v1/league/{id}/rosters`, `/v1/league/{id}/users` |
| Live matchup scores | `/v1/league/{id}/matchups/{week}` |
| Player names (lazy) | `/v1/players/nfl` (fetched once/day, cached) |

The player-name map is only fetched the first time you expand a lineup, then
cached locally for the day.

## Hosting

It's a static `index.html` — host it anywhere. On **GitHub Pages**, enable
Pages for this repo (Settings → Pages) and it's live at your Pages URL.

## Roadmap

- **Notifications** — a scheduled watcher that pings you on lead changes, a
  halftime check, and a final recap.
- **ESPN + Yahoo** leagues alongside Sleeper (these need server-side fetching
  for auth/CORS reasons, so they'll ride on the watcher).
