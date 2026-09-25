"""hermes-feishu-cardkit — CardKit streaming cards for the Feishu/Lark channel.

A drop-in platform plugin that registers over the built-in ``feishu`` platform
(last-writer-wins registry) with an adapter subclassing the built-in
FeishuAdapter. All base behaviour (WebSocket, threads, media, approvals,
pairing, comment events) is inherited; this subclass adds:

  1. ``send()``     — streaming FIRST frame (``expect_edits``) opens a CardKit
                      2.0 card bubble; a live card for the chat is RESUMED
                      (segment break) instead of opening a new one — one card
                      per turn.
  2. ``edit_message()`` — later frames stream into the card via
                      ``cardElement.content``; a finalize edit arms a DELAYED
                      commit — segment-break resumes or further tool progress
                      within the window cancel it, so the completed look
                      (🛠️ tool panel + footer) only renders at true turn end.
  3. Tool-progress folding — progress sends carrying ``tool_progress`` metadata
                      are swallowed into the card's collapsible panel instead
                      of separate bubbles.

Delayed-commit rationale: the gateway framework sends ``finalize=True`` edits at
BOTH tool boundaries (segment breaks) and turn end, indistinguishably. Waiting a
few seconds lets a resumed segment or more tool activity cancel the commit, so
multi-tool turns keep ONE live card and never flash "已完成 · 耗时 0.0s" at a
tool boundary.

Fail-safe by design: any CardKit API failure latches the card path off for the
process and the adapter degrades to the inherited text/post edit streaming —
replies are never lost. Set ``FEISHU_CARDKIT_STREAMING=0`` to disable outright.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import os
from typing import Any, Dict, Optional

from gateway.platforms.base import BasePlatformAdapter, PlatformConfig, SendResult

logger = logging.getLogger("gateway.feishu.cardkit-plugin")

# The built-in adapter module lives at plugins/platforms/feishu/adapter.py in the
# Hermes source tree. Import it (don't copy it) so the subclass tracks upstream.
try:
    from plugins.platforms.feishu.adapter import (
        FeishuAdapter,
        check_feishu_requirements,
        feishu_deps_present,
        interactive_setup,
        _apply_yaml_config,
        _is_connected,
        _standalone_send,
    )
    _BUILTIN_AVAILABLE = True
except Exception as exc:  # pragma: no cover - broken install, register nothing
    logger.error("[feishu-cardkit] built-in feishu adapter unavailable: %s", exc)
    _BUILTIN_AVAILABLE = False

# CardKit logic (card builders + API wrapper) ships in this plugin's package.
from .cardkit_stream import (
    CardKitStreamManager,
    cardkit_enabled,
    cardkit_sdk_available,
    strip_cursor_all,
)

_CURSOR = " ▉"
# Segment-break resumes arrive quickly (the next segment starts or more tools
# fire within a couple of seconds); the turn-final commits after this window.
_FINALIZE_DELAY_S = 3.5
# Quiet mode (CEO 2026-09-25): Feishu = task in → result out. Tool progress and
# status heartbeats are swallowed entirely (no bubbles, no tool panel). Opt back
# into the collapsible tool panel with FEISHU_CARDKIT_TOOL_PANEL=1.
_QUIET_DEFAULT = os.environ.get("FEISHU_CARDKIT_QUIET", "1") not in ("0", "false", "no", "off")
_TOOL_PANEL_ON = os.environ.get("FEISHU_CARDKIT_TOOL_PANEL", "0") in ("1", "true", "yes", "on")
# Sentinel id returned for swallowed quiet-mode sends so callers can detect it.
_QUIET_SENTINEL = "quiet:swallowed"


def _cardkit_available() -> bool:
    return cardkit_enabled() and cardkit_sdk_available()


if _BUILTIN_AVAILABLE:

    class FeishuCardKitAdapter(FeishuAdapter):
        """Built-in FeishuAdapter + CardKit streaming cards (one card per turn)."""

        # The card has a distinct "processing" state that MUST be closed by an
        # explicit finalize edit even when the visible text is unchanged (same
        # contract as DingTalk AI Cards).
        REQUIRES_EDIT_FINALIZE = True

        def __init__(self, config: PlatformConfig):
            super().__init__(config)
            self._cardkit_states: Dict[str, Any] = {}
            self._cardkit: Optional[CardKitStreamManager] = None
            self._cardkit_streaming_on = True  # process-lifetime latch; False after a hard failure
            self._cardkit_pending: Dict[str, asyncio.Task] = {}  # message_id → delayed finalize task

        # ── helpers ────────────────────────────────────────────────────────

        def _cardkit_streaming_available(self) -> bool:
            if self._cardkit_streaming_on is False:
                return False
            return _cardkit_available()

        def _live_card_for_chat(self, chat_id: str):
            """Most recent non-finalized card state for a chat, if any."""
            for state in reversed(list(self._cardkit_states.values())):
                if state.chat_id == chat_id and not state.finalized:
                    return state
            return None

        def _cancel_pending_finalize(self, message_id: str) -> None:
            task = self._cardkit_pending.pop(message_id, None)
            if task is not None and not task.done():
                task.cancel()

        def _last_model_hint(self) -> Optional[str]:
            """Best-effort model name for the card footer (global default model)."""
            try:
                import yaml
                from hermes_cli.config_env import get_hermes_home
                cfg_path = get_hermes_home() / "config.yaml"
                if cfg_path.exists():
                    with open(cfg_path, "r", encoding="utf-8") as fh:
                        cfg = yaml.safe_load(fh) or {}
                    model = cfg.get("model") if isinstance(cfg.get("model"), dict) else None
                    return str(model.get("default")) if isinstance(model, dict) and model.get("default") else None
            except Exception:
                pass
            return None

        # ── delayed finalize (segment break vs turn end) ───────────────────

        def _arm_delayed_finalize(self, state, text: str) -> None:
            self._cancel_pending_finalize(state.message_id)
            try:
                loop = asyncio.get_running_loop()
            except RuntimeError:
                return  # no loop (tests): never commit inline from a sync caller
            self._cardkit_pending[state.message_id] = loop.create_task(
                self._delayed_commit(state, text))

        async def _delayed_commit(self, state, text: str) -> None:
            try:
                await asyncio.sleep(_FINALIZE_DELAY_S)
            except asyncio.CancelledError:
                return  # resumed — segment break, not turn end
            self._cardkit_pending.pop(state.message_id, None)
            if state.finalized:
                return
            try:
                await self._cardkit.finalize_card(state, text, model=self._last_model_hint())
                state.finalized = True
            except Exception as exc:
                logger.warning("[CardKit] finalize failed: %s", exc)
                state.finalized = True
                with contextlib.suppress(Exception):
                    await self._cardkit.finalize_card(state, text)
                self._cardkit_streaming_on = False

        # ── streaming lifecycle ────────────────────────────────────────────

        async def send(
            self, chat_id: str, content: str, reply_to: Optional[str] = None,
            metadata: Optional[Dict[str, Any]] = None,
        ) -> SendResult:
            if not self._client:
                return SendResult(success=False, error="Not connected")

            md = metadata or {}

            # Quiet mode (default, CEO 2026-09-25): Feishu = task in → result out.
            # Tool records, status heartbeats and interim advisories are swallowed
            # BEFORE they ever hit the chat — no bubbles, no "(已编辑)" panels.
            if _QUIET_DEFAULT:
                state_q = self._live_card_for_chat(chat_id)
                if state_q is not None:
                    # the turn is alive: a pending finalize must not fire mid-work
                    self._cancel_pending_finalize(state_q.message_id)
                if md.get("tool_progress"):
                    if state_q is not None and _TOOL_PANEL_ON:
                        state_q.tool_lines.append(content.strip())
                        state_q.tool_lines = state_q.tool_lines[-40:]
                    return SendResult(success=True, message_id=_QUIET_SENTINEL)
                if md.get("_interim_send") and not md.get("expect_edits"):
                    return SendResult(success=True, message_id=_QUIET_SENTINEL)

            # Tool-progress lines (non-quiet mode): fold into the live card's
            # collapsible panel and cancel any pending finalize.
            elif md.get("tool_progress") and self._cardkit_states:
                state = self._live_card_for_chat(chat_id)
                if state is not None:
                    self._cancel_pending_finalize(state.message_id)
                    state.tool_lines.append(content.strip())
                    if len(state.tool_lines) > 40:
                        state.tool_lines = state.tool_lines[-40:]
                    return SendResult(success=True, message_id=state.message_id)

            # Streaming FIRST frame: resume a live card (segment break) or open one.
            if md.get("expect_edits") and self._cardkit_streaming_available():
                existing = self._live_card_for_chat(chat_id)
                if existing is not None:
                    # One card per turn: adopt the sealed card for the next segment.
                    self._cancel_pending_finalize(existing.message_id)
                    if not existing.sealed_text:
                        existing.sealed_text = existing.last_visible_text
                    return SendResult(success=True, message_id=existing.message_id)
                try:
                    if self._cardkit is None:
                        self._cardkit = CardKitStreamManager(self)
                    state = await self._cardkit.create_and_send_streaming_card(
                        chat_id, reply_to=reply_to, metadata=metadata)
                    self._cardkit_states[state.message_id] = state
                    while len(self._cardkit_states) > 32:  # bounded bookkeeping
                        self._cardkit_states.pop(next(iter(self._cardkit_states)), None)
                    return SendResult(success=True, message_id=state.message_id)
                except Exception as exc:
                    logger.warning("[CardKit] streaming card open failed, falling back to text: %s", exc)
                    self._cardkit_streaming_on = False

            return await super().send(chat_id, content, reply_to=reply_to, metadata=metadata)

        async def edit_message(
            self, chat_id: str, message_id: str, content: str, *, finalize: bool = False,
        ) -> SendResult:
            if message_id in self._cardkit_states and self._cardkit is not None:
                state = self._cardkit_states[message_id]
                try:
                    if state.finalized:
                        # Idempotent success for late finalize edits.
                        return SendResult(success=True, message_id=message_id)
                    clean = strip_cursor_all(content)
                    if finalize:
                        # Delayed commit: a resumed segment or more tool progress within
                        # the window cancels this; only a quiet turn end renders the
                        # completed card.
                        self._arm_delayed_finalize(state, clean)
                    else:
                        self._cancel_pending_finalize(message_id)
                        if clean and clean != state.last_pushed:
                            await self._cardkit.stream_content(state, clean)
                    return SendResult(success=True, message_id=message_id)
                except Exception as exc:
                    logger.warning("[CardKit] streaming update failed, falling back to text edit: %s", exc)
                    self._cancel_pending_finalize(message_id)
                    state.finalized = True
                    with contextlib.suppress(Exception):
                        await self._cardkit.finalize_card(state, content)
                    self._cardkit_streaming_on = False  # latch off; text path takes over

            return await super().edit_message(chat_id, message_id, content, finalize=finalize)


def _check_requirements() -> bool:
    """PASSIVE probe: built-in deps AND the cardkit SDK surface."""
    if not _BUILTIN_AVAILABLE:
        return False
    if not feishu_deps_present():
        return False
    return cardkit_sdk_available()


def register(ctx) -> None:
    """Register over the built-in feishu platform (last writer wins)."""
    if not _BUILTIN_AVAILABLE:
        logger.warning("[feishu-cardkit] built-in adapter missing; plugin not registered")
        return
    ctx.register_platform(
        name="feishu",  # same name → overrides the built-in registration
        label="Feishu / Lark (CardKit)",
        adapter_factory=FeishuCardKitAdapter,
        check_fn=_check_requirements,
        ensure_deps_fn=check_feishu_requirements,
        is_connected=_is_connected,
        validate_config=_is_connected,
        required_env=["FEISHU_APP_ID", "FEISHU_APP_SECRET"],
        install_hint="Run `hermes setup` to install Feishu support.",
        setup_fn=interactive_setup,
        apply_yaml_config_fn=_apply_yaml_config,
        allowed_users_env="FEISHU_ALLOWED_USERS",
        allow_all_env="FEISHU_ALLOW_ALL_USERS",
        cron_deliver_env_var="FEISHU_HOME_CHANNEL",
        standalone_sender_fn=_standalone_send,
        max_message_length=8000,
        emoji="🪽",
        allow_update_command=True,
    )
    logger.info("[feishu-cardkit] registered over built-in feishu platform")
