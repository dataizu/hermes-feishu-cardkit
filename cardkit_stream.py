"""CardKit streaming-card support for the Feishu adapter (2026-09-25, COO patch).

Implements OpenClaw-plugin-equivalent UX on Hermes:

  1. First streaming frame  → create a CardKit 2.0 card entity + send an
     ``interactive`` message referencing it (``{"type":"card","data":{"card_id":…}}``).
  2. Subsequent frames      → ``cardElement.content`` cumulative updates with a
     monotonic ``sequence`` (client-side typewriter diff).
  3. Finalize               → full card replace (``card.update``) with a completed
     look + footer (status / elapsed / model), then ``card.settings`` to close
     ``streaming_mode`` so the card behaves normally again (forward, callbacks).

Everything is opt-in via ``FEISHU_CARDKIT_STREAMING`` (default on) and fails
SAFE: any CardKit API error falls back to the adapter's existing text/post
edit path — a missing permission degrades to today's behaviour, never a
dropped reply.
"""

from __future__ import annotations

import json
import logging
import time
from dataclasses import dataclass, field
from typing import Any, Dict, Optional

logger = logging.getLogger("gateway.feishu.cardkit")

STREAMING_ELEMENT_ID = "streaming_content"
_LOADING_ICON_KEY = ""  # empty = skip the loading icon (avoid depending on an app-uploaded image key)

# ZCode-calibrated streaming hygiene: ≥1s between element pushes (client-side
# typewriter diff covers the gap), 15s per-call timeout, 3-attempt breaker.
_PUSH_MIN_INTERVAL = 1.0

# Fallbacks keep the card renderable even when the fancy bits are unavailable.
_FOOTER_ENABLED_DEFAULT = True


def cardkit_enabled(env_get: Any = None) -> bool:
    """FEISHU_CARDKIT_STREAMING gate; default ON, ``0/false/no/off`` disables."""
    getter = env_get or _env_get
    try:
        raw = getter("FEISHU_CARDKIT_STREAMING")
        if raw is None or raw == "":
            return True
        return str(raw).strip().lower() not in {"0", "false", "no", "off"}
    except Exception:
        return True


def cardkit_sdk_available() -> bool:
    """The installed lark-oapi SDK must expose the cardkit v1 API surface."""
    try:
        import lark_oapi.api.cardkit  # noqa: F401
        return True
    except Exception:
        return False


def footer_enabled(env_get: Any = None) -> bool:
    """FEISHU_CARDKIT_FOOTER gate (status/elapsed footer on the completed card)."""
    getter = env_get or _env_get
    try:
        raw = getter("FEISHU_CARDKIT_FOOTER")
        if raw is None or raw == "":
            return _FOOTER_ENABLED_DEFAULT
        return str(raw).strip().lower() not in {"0", "false", "no", "off"}
    except Exception:
        return _FOOTER_ENABLED_DEFAULT


def _env_get(key: str) -> Optional[str]:
    import os
    return os.environ.get(key)


def _fmt_elapsed(ms: float) -> str:
    seconds = ms / 1000.0
    if seconds < 60:
        return f"{seconds:.1f}s"
    minutes = int(seconds // 60)
    return f"{minutes}m {round(seconds % 60)}s"


# ── Card markdown style optimization (ported from OpenClaw markdown-style.js) ──
# Feishu cards render # headings as huge blue text and jam tables against
# surrounding prose; demote headings and add <br> spacing so long answers read well.

_CODE_BLOCK_RE = None  # compiled lazily


def optimize_markdown_style(text: str) -> str:
    """Heading demotion + table/code spacing fixups for card rendering.

    Ported from the official OpenClaw Lark plugin (markdown-style.js):
    - H1→H4, H2~H6→H5 (only when the text actually has h1~h3)
    - <br> spacing before/after tables and code blocks
    - 3+ consecutive newlines → 2
    Code block CONTENT is protected via placeholders and never modified.
    """
    import re as _re

    try:
        # 1. Protect code blocks
        code_blocks: list = []
        if _CODE_BLOCK_RE is None:
            pattern = _re.compile(r"(^|\n)(`{3,})([^\n]*)\n[\s\S]*?\n\2(?=\n|$)")
        else:
            pattern = _CODE_BLOCK_RE

        def _stash(m: "_re.Match") -> str:
            block = m.group(0)
            prefix = m.group(1) or ""
            code_blocks.append(block[len(prefix):])
            return f"{prefix}___CB_{len(code_blocks) - 1}___"

        r = pattern.sub(_stash, text)

        # 2. Heading demotion (order matters: H2~H6 first, then H1)
        if _re.search(r"^#{1,3} ", r, _re.M):
            r = _re.sub(r"^#{2,6} .+$", lambda m: "##### " + m.group(0).split(" ", 1)[1], r, flags=_re.M)
            r = _re.sub(r"^# .+$", lambda m: "#### " + m.group(0).split(" ", 1)[1], r, flags=_re.M)

        # 3. Spacing between consecutive demoted headings
        r = _re.sub(r"^(#{4,5} .+)\n{1,2}(#{4,5} )", r"\1\n<br>\n\2", r, flags=_re.M)

        # 4. Table spacing: blank line before a table block
        r = _re.sub(r"^([^|\n].*)\n(\|.+\|)", r"\1\n\n\2", r, flags=_re.M)
        # <br> before table blocks
        r = _re.sub(r"\n\n((?:\|.+\|[^\S\n]*\n?)+)", r"\n\n<br>\n\n\1", r)
        # <br> after table blocks (skip when followed by rule/heading/bold/EOF)
        def _after_table(m: "_re.Match") -> str:
            table = m.group(0)
            after = r[m.end():].lstrip("\n")
            if not after or after.startswith(("---", "#### ", "##### ", "**")):
                return table
            return table + "\n<br>\n"
        r = _re.sub(r"(?:(?:^\|.+\|[^\S\n]*\n?)+)", _after_table, r, flags=_re.M)

        # 5. Restore code blocks with <br> spacing
        for i, block in enumerate(code_blocks):
            r = r.replace(f"___CB_{i}___", f"\n<br>\n{block}\n<br>\n")

        # 6. Compress excess blank lines
        r = _re.sub(r"\n{3,}", "\n\n", r)
        return r
    except Exception:
        return text  # never break the reply over styling


def chunk_card_text(text: str, limit: int = 3800) -> "list[str]":
    """Split long text into card-element-sized chunks at paragraph boundaries.

    Keeps each markdown element comfortably under card limits so very long
    replies render as multiple stacked elements instead of failing card.update.
    """
    if len(text) <= limit:
        return [text]
    chunks: list = []
    cur = ""
    for para in text.split("\n\n"):
        candidate = f"{cur}\n\n{para}" if cur else para
        if len(candidate) > limit and cur:
            chunks.append(cur)
            cur = para
        else:
            cur = candidate
    if cur:
        chunks.append(cur)
    return chunks


def build_streaming_card(*, show_loading_icon: bool = False) -> Dict[str, Any]:
    """Initial CardKit 2.0 streaming card: empty streaming element + optional loading icon."""
    elements: list = [{
        "tag": "markdown",
        "content": "",
        "text_align": "left",
        "text_size": "normal_v2",
        "element_id": STREAMING_ELEMENT_ID,
    }]
    if show_loading_icon and _LOADING_ICON_KEY:
        elements.append({
            "tag": "markdown",
            "content": " ",
            "icon": {"tag": "custom_icon", "img_key": _LOADING_ICON_KEY, "size": "16px 16px"},
            "element_id": "loading_icon",
        })
    return {
        "schema": "2.0",
        "config": {
            "streaming_mode": True,
            "locales": ["zh_cn", "en_us"],
            "summary": {
                "content": "Processing...",
                "i18n_content": {"zh_cn": "处理中...", "en_us": "Processing..."},
            },
        },
        "body": {"elements": elements},
    }


def build_tool_panel(tool_lines: "list[str]") -> Optional[Dict[str, Any]]:
    """Collapsible 🛠️ tool-summary panel (default COLLAPSED) for the completed card.

    Mirrors the OpenClaw/ZCode UX: tool-call transcript folds away under a grey
    notation-sized header so the answer reads clean; tap to expand.
    """
    if not tool_lines:
        return None
    body = "\n".join(f"· {line}" for line in tool_lines)
    zh_title = f"工具摘要（{len(tool_lines)}）"
    en_title = f"Tool Summary ({len(tool_lines)})"
    return {
        "tag": "collapsible_panel",
        "expanded": False,
        "header": {
            "title": {
                "tag": "plain_text",
                "content": f"🛠️ {en_title}",
                "i18n_content": {"zh_cn": f"🛠️ {zh_title}", "en_us": f"🛠️ {en_title}"},
                "text_color": "grey",
                "text_size": "notation",
            },
            "vertical_align": "center",
            "icon": {
                "tag": "standard_icon",
                "token": "down-small-ccm_outlined",
                "color": "grey",
                "size": "16px 16px",
            },
            "icon_position": "right",
            "icon_expanded_angle": -180,
        },
        "border": {"color": "grey", "corner_radius": "5px"},
        "background_style": {"style": "grey"},
        "vertical_spacing": "8px",
        "padding": "8px 12px 8px 12px",
        "elements": [
            {
                "tag": "markdown",
                "content": body,
                "text_size": "notation",
            }
        ],
    }


def build_completed_card(text: str, *, elapsed_ms: Optional[float], model: Optional[str] = None,
                         is_error: bool = False, show_footer: bool = True,
                         tool_lines: Optional["list[str]"] = None) -> Dict[str, Any]:
    """Terminal card: optional collapsed tool panel + full answer + footer.

    Long answers render as multiple stacked markdown elements (card limits),
    each styled via optimize_markdown_style (heading demotion + table spacing).
    """
    elements: list = []
    tool_panel = build_tool_panel(tool_lines or [])
    if tool_panel is not None:
        elements.append(tool_panel)
    styled = optimize_markdown_style(text) if text else ""
    elements.extend(
        {"tag": "markdown", "content": chunk, "text_align": "left", "text_size": "normal_v2"}
        for chunk in (chunk_card_text(styled) or ["（无内容）"])
    )
    if show_footer:
        primary: list = []
        if is_error:
            primary.append("出错")
        else:
            primary.append("已完成")
        if elapsed_ms is not None:
            primary.append(f"耗时 {_fmt_elapsed(elapsed_ms)}")
        if model:
            primary.append(model)
        elements.append({
            "tag": "markdown",
            "content": " · ".join(primary),
            "i18n_content": {"zh_cn": " · ".join(primary), "en_us": " · ".join(primary)},
            "text_size": "notation",
        })
    return {
        "schema": "2.0",
        "config": {
            "streaming_mode": False,
            "locales": ["zh_cn", "en_us"],
            "update_multi": True,
            "summary": {
                "content": "Completed",
                "i18n_content": {"zh_cn": "已完成", "en_us": "Completed"},
            },
        },
        "body": {"elements": elements},
    }


@dataclass
class CardStreamState:
    """Book-keeping for ONE streaming card within a chat turn."""

    card_id: str
    message_id: str = ""
    chat_id: str = ""
    seq: int = 1
    start_ts: float = field(default_factory=time.monotonic)
    last_pushed: str = ""
    last_push_ts: float = 0.0
    fail_count: int = 0
    finalized: bool = False  # sealed by a finalize edit; later finalize edits short-circuit
    failed: bool = False  # latched on first hard failure → caller falls back for the rest of the turn
    tool_lines: "list[str]" = field(default_factory=list)  # folded tool-progress lines (collapsible panel)


class CardKitStreamManager:
    """Per-adapter CardKit API wrapper. All API calls go through the adapter's
    ``_run_blocking`` executor so the SDK's blocking transport stays off the loop."""

    def __init__(self, adapter: Any) -> None:
        self._adapter = adapter

    # ── low-level API helpers ──────────────────────────────────────────────

    def _client(self):
        client = getattr(self._adapter, "_client", None)
        if client is None:
            raise RuntimeError("Feishu adapter not connected")
        return client

    async def _api(self, fn, *args):
        return await self._adapter._run_blocking(fn, *args)

    async def create_and_send_streaming_card(
        self, chat_id: str, *, reply_to: Optional[str] = None, metadata: Optional[Dict[str, Any]] = None,
    ) -> CardStreamState:
        """create card entity → send interactive message referencing it."""
        from lark_oapi.api.cardkit.v1 import CreateCardRequest, CreateCardRequestBody
        from lark_oapi.api.cardkit.v1.model.card import Card as _CardModel

        card_json = build_streaming_card()
        body = (CreateCardRequestBody.builder()
                .type("card_json")
                .data(json.dumps(card_json, ensure_ascii=False))
                .build())
        req = CreateCardRequest.builder().request_body(body).build()
        resp = await self._api(self._client().cardkit.v1.card.create, req)
        if not getattr(resp, "success", lambda: False)():
            raise RuntimeError(f"card.create failed: {getattr(resp, 'code', '?')} {getattr(resp, 'msg', '')}")
        card_id = getattr(getattr(resp, "data", None), "card_id", None)
        if not card_id:
            raise RuntimeError("card.create returned no card_id")

        # Send the interactive message that links the card to the chat.
        payload = json.dumps({"type": "card", "data": {"card_id": card_id}}, ensure_ascii=False)
        response = await self._adapter._feishu_send_with_retry(
            chat_id=chat_id, msg_type="interactive", payload=payload, reply_to=reply_to, metadata=metadata,
        )
        result = self._adapter._finalize_send_result(response, "cardkit interactive send failed")
        state = CardStreamState(card_id=card_id)
        state.message_id = result.message_id or ""
        state.chat_id = chat_id
        if not result.success or not state.message_id:
            raise RuntimeError("cardkit interactive send returned no message_id")
        return state

    async def stream_content(self, state: CardStreamState, content: str) -> None:
        """Cumulative text push to the streaming element (typewriter diff on the client).

        ZCode-calibrated throttling: ≥1s between pushes (the client-side diff animates
        in between), single-call timeout 15s, exponential backoff 1s→2s→4s, circuit
        breaker after 3 consecutive failures.
        """
        from lark_oapi.api.cardkit.v1 import ContentCardElementRequest, ContentCardElementRequestBody

        now = time.monotonic()
        wait = state.last_push_ts + _PUSH_MIN_INTERVAL - now
        if wait > 0:
            await asyncio.sleep(wait)

        last_exc: Optional[Exception] = None
        for attempt in range(3):
            try:
                state.seq += 1
                body = (ContentCardElementRequestBody.builder()
                        .content(content)
                        .sequence(state.seq)
                        .build())
                req = (ContentCardElementRequest.builder()
                       .card_id(state.card_id)
                       .element_id(STREAMING_ELEMENT_ID)
                       .request_body(body)
                       .build())
                resp = await asyncio.wait_for(
                    self._api(self._client().cardkit.v1.card_element.content, req), timeout=15.0)
                if not getattr(resp, "success", lambda: False)():
                    raise RuntimeError(f"cardElement.content failed: {getattr(resp, 'code', '?')} {getattr(resp, 'msg', '')}")
                state.last_pushed = content
                state.last_push_ts = time.monotonic()
                state.fail_count = 0
                return
            except Exception as exc:
                last_exc = exc
                state.seq -= 1  # roll back the reserved sequence for a clean retry
                if attempt < 2:
                    await asyncio.sleep(2 ** attempt)
        state.fail_count += 3  # exhausted retries → circuit breaker trips in the adapter
        raise last_exc or RuntimeError("cardElement.content failed")

    async def finalize_card(
        self, state: CardStreamState, text: str, *, is_error: bool = False, model: Optional[str] = None,
    ) -> None:
        """Replace the card with the completed look (tool panel + footer) and close streaming mode."""
        from lark_oapi.api.cardkit.v1 import UpdateCardRequest
        from lark_oapi.api.cardkit.v1.model.card import Card as _CardModel
        from lark_oapi.api.cardkit.v1.model.update_card_request_body import UpdateCardRequestBody

        elapsed_ms = (time.monotonic() - state.start_ts) * 1000.0
        show_footer = footer_enabled()
        card_json = build_completed_card(text, elapsed_ms=elapsed_ms, model=model, is_error=is_error,
                                         show_footer=show_footer, tool_lines=state.tool_lines)
        state.seq += 1
        card_model = _CardModel.builder().type("card_json").data(json.dumps(card_json, ensure_ascii=False)).build()
        body = (UpdateCardRequestBody.builder()
                .card(card_model)
                .sequence(state.seq)
                .build())
        req = UpdateCardRequest.builder().card_id(state.card_id).request_body(body).build()
        resp = await self._api(self._client().cardkit.v1.card.update, req)
        if not getattr(resp, "success", lambda: False)():
            raise RuntimeError(f"card.update failed: {getattr(resp, 'code', '?')} {getattr(resp, 'msg', '')}")

        # Close streaming mode (best-effort; failure doesn't affect content).
        try:
            state.seq += 1
            from lark_oapi.api.cardkit.v1 import SettingsCardRequest
            from lark_oapi.api.cardkit.v1.model.settings_card_request_body import SettingsCardRequestBody
            sbody = (SettingsCardRequestBody.builder()
                     .settings(json.dumps({"streaming_mode": False}, ensure_ascii=False))
                     .sequence(state.seq)
                     .build())
            sreq = SettingsCardRequest.builder().card_id(state.card_id).request_body(sbody).build()
            await self._api(self._client().cardkit.v1.card.settings, sreq)
        except Exception as exc:  # noqa: BLE001
            logger.debug("[CardKit] close streaming_mode best-effort failed: %s", exc)

    # ── cursor handling ────────────────────────────────────────────────────

    @staticmethod
    def strip_cursor(text: str, cursor: str) -> str:
        if cursor and text.endswith(cursor):
            return text[: -len(cursor)]
        return text


def _extract_message_id(response: Any) -> Optional[str]:
    data = getattr(response, "data", None)
    return getattr(data, "message_id", None) if data is not None else None
