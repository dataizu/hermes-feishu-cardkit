# hermes-feishu-cardkit

[中文说明](README.zh.md) | English

**CardKit streaming cards for the Hermes Agent Feishu/Lark channel** — the
OpenClaw-plugin-style chat experience, as a drop-in Hermes platform plugin.

Replies in Feishu render as **CardKit 2.0 streaming cards**:

- 🌊 **Typewriter streaming** — text streams into the card live
- 🛠️ **Collapsible tool panel** — tool-call progress folds into a grey
  "工具摘要（N）" panel (collapsed by default, tap to expand)
- ⏱️ **Status footer** — `已完成 · 耗时 8.4s · model-name` on completion
- 📐 **Card markdown polish** — heading demotion + table/code spacing so long
  answers read well inside cards
- 🔒 **Fail-safe** — any CardKit API error silently falls back to the built-in
  text streaming; replies are never lost

The plugin **registers over the built-in `feishu` platform** (last-writer-wins
registry), so your existing `FEISHU_APP_ID` / `FEISHU_APP_SECRET` env, allowlists,
home channel, and config keep working unchanged.

## Requirements

- Hermes Agent with the built-in Feishu channel working (this plugin subclasses
  the built-in adapter)
- `lark-oapi` SDK with the `cardkit.v1` API (shipped with recent Hermes; the
  plugin probes and falls back if absent)
- Your Feishu app needs the CardKit permissions (`cardkit:card` etc.) — most
  apps created via `hermes gateway setup` have them; verify in the Feishu
  developer console → Permissions

## Install

```bash
# from a local checkout
hermes plugins install /path/to/hermes-feishu-cardkit

# from GitHub (once pushed)
hermes plugins install <owner>/hermes-feishu-cardkit
hermes plugins enable hermes-feishu-cardkit
```

Then restart the gateway: `hermes gateway restart`.

> NOTE: the plugin needs the gateway's tool-progress lines to carry a
> `tool_progress` metadata marker. On stock Hermes (without that one-line
> framework patch) the card still streams and shows the footer — only the
> collapsible tool panel stays empty. See "Patches" below.

## Configuration

All optional — the plugin works out of the box.

| Env var | Default | Effect |
|---|---|---|
| `FEISHU_CARDKIT_STREAMING` | on | `0`/`false` disables CardKit cards entirely |
| `FEISHU_CARDKIT_FOOTER` | on | `0`/`false` hides the status/elapsed footer |

## Patches

`patches/run_turn_runner.tool_progress.patch` adds one line to
`gateway/run_turn_runner.py` (`_send_progress_text` gains a `tool_progress`
metadata marker) so tool-progress lines can be folded into the card's
collapsible panel. Apply with:

```bash
cd <hermes-agent source>
git apply patches/run_turn_runner.tool_progress.patch   # or copy the patched function
hermes gateway restart
```

Without it: streaming + footer work; the tool panel just stays empty.

## How it works

```
first streaming frame ──► cardkit.cards.create ──► interactive message (card_id)
        │                                              │
        ▼                                              ▼
text deltas ──► cardElement.content (cumulative, seq++, 1s throttle)
        │
        ▼
finalize ──► card.update (completed card: tool panel + footer) ──► card.settings(streaming_mode off)
```

- Segment breaks (tool boundaries) seal the current card; the next segment opens
  a fresh card (seal-and-continue, the pattern ZCode's Feishu bot uses).
- Streaming hygiene (calibrated on ZCode's implementation): ≥1s between pushes,
  15s per-call timeout, 1s→2s→4s backoff, 3-failure circuit breaker.
- `REQUIRES_EDIT_FINALIZE = True` mirrors the DingTalk AI-Card contract so the
  finalize edit is never skipped when content is unchanged.

## License

MIT
