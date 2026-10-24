# daftcron

Watches daft.ie for rental listings matching your criteria and pushes the good
ones to Telegram, so you find out about a property while it is still available
rather than after it is gone.

Built for a specific deadline: a lease ending 6 January 2027, in a market where
a desirable listing can be gone within hours.

---

## Data source — read this first

daftcron reads **the alert emails Daft sends you**.

It never makes an automated request to daft.ie. That matters, because
`daft.ie/legal/terms-of-use` prohibits:

> **(c)** use any robot, spider, website search/retrieval application, or other
> automated device, process or means to access, retrieve or index any portion of
> the Website and/or the Daft Service;
>
> **(d)** access, retrieve or index any portion of the Website and/or the Daft
> Service for purposes of constructing or populating any database;

Clause (d) describes precisely what a listings database is. An earlier version of
this tool polled `daft.ie/api/sitemap/`, which `robots.txt` permits with
`Allow: /api/sitemap*`. **`robots.txt` is a crawler directive, not a licence** —
it does not grant what the Terms of Use withhold. That path is retained in the
code but disabled; it refuses to run without `--force`.

The email route has a genuine engineering advantage too, not just a legal one:
**your mail provider buffers**. Daft's mail server holds the alert whether your
computer is awake or not, so a sleeping laptop delays your alerts but never loses
them. The sitemap route could not promise that — a rolling newest-200 window
scrolls past listings that were published while the machine was off.

---

## What the real alert email contains

Verified against a live alert on **4 October 2026**. Everything the parser does is
now grounded in this rather than in a guess.

**One listing per email.** The subject names the property and the body carries a
single card. Daft's alerts are **per-listing, not a daily digest** — the feed is
genuinely fast, which makes daftcron a *judgment layer* on top of an already-fast
feed rather than the thing providing speed.

**Available:**

| Field | Where it comes from |
|---|---|
| Ad id and URL | the listing link |
| Address, including Eircode | `address-title-string` paragraph |
| Price | "€1,300 per month" |
| Beds · baths · type | "1 Bed · 1 Bath · Apartment" |
| Photo URL | signed `media.daft.ie` link |
| Send time | the email `Date` header |

**Not available:**

- **BER.** Daft ships it only as an image filename — `alt="BER Rating SI_666"`,
  `src=".../ber/SI_666.png"`. The code-to-band mapping is not published, so it
  cannot be read. `scoring.w_ber` is therefore **0**: scoring an unreadable field
  would be pure noise, and flagging its absence would dock 20 points off every
  single listing. Put the weight back if the mapping ever becomes known.
- **Seller type, view count, description.** Absent, so the private-seller and
  thin-listing scam heuristics cannot fire. The price-versus-area-median check
  still works, and that is the strongest signal anyway.
- **Coordinates.** The Eircode is present, which is why the commute table is keyed
  on the address slug rather than on lat/lng.

**Order matters.** The photo link comes first, then the details, then a second
"VIEW THIS AD" link to the same ad. The parser reads the block *between* those two
links. Reversing the order breaks extraction — which is precisely how the first
version of the test fixture was written, wrongly. The fixtures in `tests/` now
mirror the real markup.

---

## Setup

### 1. Create the Daft alert

On daft.ie, run a search, then **Save this search** and turn on email alerts.
This is the data feed. Do it even if you never run daftcron: it costs nothing
and covers you meanwhile.

**Set the alert WIDER than the app's filter, never narrower.**

The Daft alert decides *what you ever see*. The app only decides *what ranks
well*. If the alert is the narrow thing, the app can never rescue a listing it
was never sent. Widening the alert costs a little parsing time; narrowing it
costs you listings, silently.

| Daft alert | `config.toml` | Why they differ |
|---|---|---|
| Min beds: one lower than you need | `min_beds = 2` | Bed counts are frequently mislisted; let the app enforce your real floor |
| Max price: ~15% above your ceiling | `max_price = 2500` | Catches overpriced places that get reduced into range |
| BER: any | `min_ber = ""` | Many private rentals publish no BER at all |
| Property type: all | `property_types = []` | Don't pre-exclude on a field that is often wrong |
| Area: your county **plus commuter counties** | `counties = ["Dublin"]` | Daft's area tree is finer than a county; a wider net is cheap here |
| Sort: **newest first** | — | Freshest listings score highest, so you want them first |

⚠️ **If you tighten the Daft alert later, update this reasoning too.** Narrowing
it silently reduces coverage, and nothing in the app will warn you — it only
ever sees what the alert sends.

**Two things worth checking once the first alert arrives:**

1. **Is it instant, or a daily digest?** This determines your real latency. A
   digest at 08:00 undermines the whole speed premise — if that's what Daft
   sends, the value shifts from "be first" to "triage well".
2. **What fields does it actually contain?** Run `daftcron.py dump-email` on a
   real one. The parser can only read what the email carries.

### 2. Get the emails into daftcron

Two options. Pick one by setting `ingest.source` in `config.toml`.

**`source = "email"`** — drop `.eml` files into `mail/`.

- **Mail.app** — select a message, then `File ▸ Save As…`, and choose
  **Raw Message Source** as the format. Save into `mail/`.
- **A mail rule** — filter on sender containing `daft.ie` and move to a mailbox
  you export from.

Simple, but it only works while Mail.app is open and running its rules.

**`source = "imap"`** — read straight from the mailbox.

IMAP is a standard, supported way to access your own mail — it is how desktop
mail clients work. Reading your own mailbox over IMAP is not something Google
objects to, and polling it every 30 minutes is unremarkable.

What *does* draw Gmail's attention is a specific set of things, and **polling is
not one of them**:

| Draws attention | Doesn't |
|---|---|
| Repeated **failed logins** → temporary lockout | Polling every 30 minutes |
| Using the **account password** — no longer works at all | Reading your own mail over IMAP |
| Logging in from a **sudden new country or datacenter IP** | Keeping a persistent connection |

That third row matters for your hosting choice: **running this on a VPS is more
likely to trip Google's login-anomaly check than running it from home**, because
the login arrives from a datacenter address. From your Mac or a home Pi it is a
non-event.

Setup:

1. Google Account ▸ Security ▸ **2-Step Verification** — must be ON.
2. Google Account ▸ Security ▸ **App passwords** — create one, copy the
   16-character value.
3. Store it — **not** in `config.toml`:

```sh
export DAFTCRON_IMAP_PASSWORD='xxxx xxxx xxxx xxxx'
# or, once, in the macOS Keychain:
security add-generic-password -s daftcron -a you@gmail.com -w
```

4. Set `ingest.source = "imap"` and `ingest.imap_user` in `config.toml`.
5. Verify before scheduling anything:

```sh
python3 daftcron.py check-imap
```

That reports the mailbox size and how many messages match, **without marking
anything read or processing anything**. If it says `matching 0`, send a test
alert or widen `imap_search` to just `UNSEEN`.

Handled mail is marked read in Gmail. Set `imap_archive_folder` to also move it
into a label (e.g. `"Daft"`), or leave it empty to keep it in the inbox.

### 3. Configure

Edit `config.toml`. The sections that matter most:

```toml
[filters]
max_price = 2500
min_beds = 2
counties = ["Dublin"]      # empty list = all of Ireland

[scoring]
alert_threshold = 55       # raise to get fewer, better alerts
```

### 4. Telegram

1. Message [@BotFather](https://t.me/BotFather), send `/newbot`, and copy the token.
2. Message your new bot once, then find your chat id at
   `https://api.telegram.org/bot<TOKEN>/getUpdates`.
3. Put them in `config.toml`, or better, export them so they never touch disk:

```sh
export DAFTCRON_TELEGRAM_TOKEN=...
export DAFTCRON_TELEGRAM_CHAT=...
```

4. Confirm: `python3 daftcron.py test-telegram`

### 5. First run

```sh
python3 daftcron.py dump-email mail/some-alert.eml   # check what it can read
python3 daftcron.py ingest --dry-run                 # score without alerting
python3 daftcron.py top 20                           # see the ranking
open out/report.html                                 # browse it properly
```

Once the extraction looks right, drop `--dry-run`.

---

## Commands

| Command | Does |
|---|---|
| `ingest` | Read alert emails, score, alert. The normal entry point. |
| `ingest --dry-run` | Same, but sends nothing. |
| `check-imap` | Verify mailbox access without processing or marking read. |
| `dump-email FILE` | Show exactly what is extracted from one `.eml`. Use this to tune. |
| `top [N]` | Print the current best listings. |
| `report` | Rewrite `out/report.html`. |
| `stats` | Coverage summary. |
| `export` | Dump everything to `out/matches.csv`. |
| `test-telegram` | Verify credentials. |
| `baseline` | Seed the seen-set without alerting. |
| `scan --force` | **Not compliant.** The disabled sitemap path. |

---

## How the score works

Hard filters run first — anything outside `max_price`, `min_beds`, `counties`,
or `min_ber` is discarded and never scored. Survivors get 0-100:

| Component | Max | What earns it |
|---|---|---|
| Price headroom | 20 | How far under your ceiling |
| BER | 25 | A/B strong, F/G penalised as high heating cost |
| Value | 25 | €/bed against the median for that area |
| Freshness | 15 | Published today scores full marks |
| Price cut | 15 | Detected reduction since first seen |
| Scam penalty | −20 each | See below |

**Area medians need volume.** The value component compares against the median
€/bed for that area, which requires at least 3 listings in that area. On a fresh
database it stays dormant for the first few runs, then switches on by itself.

### Scam flags

The Irish rental market has a persistent deposit-fraud problem, and a listing
that is *too* good is the warning sign. daftcron raises a flag when a listing is
priced below 60% of its area median, when a private seller has almost no photos,
or when the description contains phrases like "currently abroad", "no viewing",
or "deposit before viewing". Flags subtract 20 points each, so a suspicious
listing sinks down the ranking instead of topping it.

---

## Scheduling, and whether your computer must stay on

**Short answer: no, your computer does not have to stay on.** But there is a
real trade to understand.

`launchd` is macOS's scheduler. It **does not run jobs while the machine is
asleep or shut down**. If a scheduled run is missed, it runs once when the Mac
next wakes — a catch-up, not a silent gap. So a sleeping laptop gives you late
alerts, not missing ones.

That is tolerable here specifically *because* the feed is email. Daft's server
mails you the alert; your mail provider holds it regardless of your machine's
state. Nothing is lost while you sleep, unlike with a scraped feed that scrolls
away.

What you lose is **latency**. If the Mac sleeps from 23:00 to 08:00, a listing
posted at 02:00 reaches you at 08:00. In this market that can be the difference
between a viewing and a "sorry, it's gone".

### Options, cheapest first

| Approach | Always on | Cost | Notes |
|---|---|---|---|
| **Mac, runs when you open it** | No | free | Zero setup. Fine if you're at the machine daily. |
| **Scheduled wake** | Mostly | free | `sudo pmset repeat wakeorpoweron MTWRFSU 07:00:00` then let launchd run in that window. Needs mains power; a closed lid may still sleep. |
| **Mac stays awake** | Yes | electricity | `caffeinate -s` while plugged in. Simplest true 24/7. |
| **Raspberry Pi** | Yes | ~€60 once | Silent, ~€5/yr to run, purpose-built for this. |
| **Small VPS** | Yes | ~€4–6/mo | Independent of your home entirely. curl works; see the note below. |

Whichever you pick, **delivery is decoupled already**: once daftcron sends to
Telegram, the message lands on your phone regardless of what runs where.

### Installing the launchd job

```sh
cp ie.atensai.daftcron.plist ~/Library/LaunchAgents/
launchctl load -w ~/Library/LaunchAgents/ie.atensai.daftcron.plist
launchctl list | grep daftcron     # confirm it registered
```

The job runs `ingest` **hourly**. When there are no new emails it exits in
seconds, so the interval is cheap. It also runs once at login (`RunAtLoad`), and
survives reboots — macOS loads everything in `~/Library/LaunchAgents` at login.

---

## Checking on it

Everything below assumes you are in the project folder:

```sh
cd "/Users/jidemobell/Documents/code_files/Atensai/ideabox/ideas/implemented/daftcron"
```

### One command for the whole picture

```sh
python3 daftcron.py status
```

Reports whether Telegram still works, whether IMAP still authenticates,
**whether the scheduled job is loaded and whether its last run succeeded**, when
it last ingested, and what is in the database.

### Is the schedule alive?

```sh
launchctl list | grep daftcron
```

Read the output as `PID  LAST-EXIT  LABEL`:

| Output | Meaning |
|---|---|
| `-  0  ie.atensai.daftcron` | Loaded, idle, last run succeeded — what you want |
| `1234  0  ie.atensai.daftcron` | Running right now |
| `-  1  ie.atensai.daftcron` | **Last run failed** — check `out/daftcron.err` |
| *(no output)* | Not loaded — nothing is running automatically |

### Has it actually been running?

```sh
tail -20 out/daftcron.log     # what it did, and when
cat out/daftcron.err          # errors only — should be empty
python3 daftcron.py stats     # run history from the database
```

### Run it by hand

```sh
python3 daftcron.py ingest --dry-run    # score but send nothing
python3 daftcron.py ingest              # for real
python3 daftcron.py top 10              # the ranking, in the terminal
open out/report.html                    # the same, browsable
```

To trigger it through the scheduler rather than directly:

```sh
launchctl kickstart -p gui/$(id -u)/ie.atensai.daftcron
```

### Stop or restart it

```sh
launchctl unload -w ~/Library/LaunchAgents/ie.atensai.daftcron.plist   # stop
launchctl load -w   ~/Library/LaunchAgents/ie.atensai.daftcron.plist   # start
```

### Diagnosing the parts

```sh
python3 daftcron.py test-telegram   # can it still send to your phone?
python3 daftcron.py check-imap      # can it still read Gmail?
python3 daftcron.py cadence         # per-listing or digest, and is the parser coping?
python3 daftcron.py dump-email FILE # what it extracts from one email
```

### A failure you may see

`launchd` fires jobs at login and on wake, sometimes **before the network is
ready**. That surfaces as:

```
Could not reach imap.gmail.com:993 — gaierror: [Errno 8] nodename nor servname provided
```

The connection is retried with backoff, which absorbs most of it. If you see it
on *every* run, the host or network is genuinely wrong. If it appears once after
a reboot, it is the startup race and the next hourly run will be fine.

---

## Notes on the environment

- **Zero dependencies.** Standard library only; nothing to `pip install`, so it
  will still run in two years.
- **Python 3.11+** is required, for `tomllib`.
- If you ever re-enable the sitemap path, note that daft.ie's WAF rejects
  Python's TLS fingerprint with HTTP 403, and also rejects `curl --http1.1`.
  The code therefore shells out to `curl` without that flag. Both behaviours
  were verified flag-by-flag on 2026-10-02.

## One thing deliberately not built

- **Commute times.** The scoring covers price, BER, value, freshness and risk.
  Travel time to a fixed destination needs a routing API and a key; the
  `lat`/`lng` fields are already captured for when that is wanted.

---

*Email field extraction is provisional — it was written against a synthetic
fixture, not a real Daft alert. Run `dump-email` on an actual alert before
trusting the numbers, and tune the `EMAIL_*` patterns in `daftcron.py` if the
layout differs.*
