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
    build_cron_card,
    build_clarify_card,
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
# Cron envelope the upstream scheduler splices into delivered text (scheduler_delivery.py:
# "Cronjob Response: {name}\n(job_id: {id})\n----...\n\n{content}\n\nTo stop or manage...").
# The framework does NOT pass job_id via metadata yet (upstream issue #26004), so detect the
# envelope in the text itself.
_CRON_ENVELOPE_PREFIX = "Cronjob Response: "
_QUIET_SENTINEL = "quiet:swallowed"


def _parse_cron_envelope(content: str):
    """Split the scheduler's cron envelope → (task_name, body) or None if not a cron send."""
    text = content.strip()
    if not text.startswith(_CRON_ENVELOPE_PREFIX):
        return None
    lines = text.splitlines()
    task_name = lines[0][len(_CRON_ENVELOPE_PREFIX):].strip()
    # drop "(job_id: ...)" and the "-----" separator lines
    body_lines = [l for l in lines[1:] if not l.strip().startswith("(job_id:")
                  and not set(l.strip()) <= {"-"}]
    body = "\n".join(body_lines).strip()
    # strip the trailing management footer
    for marker in ("To stop or manage this job",):
        idx = body.find(marker)
        if idx > 0:
            body = body[:idx].strip()
    return task_name, body


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
            # clarify button cards (CEO 2026-09-26): clarify_id → bookkeeping dict
            self._clarify_cards: Dict[str, Dict[str, Any]] = {}
            self._clarify_card_ids: Dict[str, str] = {}  # message_id → card_id (filled by manager)

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

            # Cron/scheduled deliveries: render as a static card so every Feishu surface
            # has the unified look (CEO 2026-09-26). Two detection paths:
            #   a) metadata carries job_id (future: upstream issue #26004 lands)
            #   b) the scheduler's text envelope "Cronjob Response: ..." (today's reality)
            is_cron = bool(md.get("job_id"))
            cron_task = md.get("job_name")
            cron_body = content
            if not is_cron:
                parsed = _parse_cron_envelope(content) if _cardkit_streaming_available() else None
                if parsed:
                    is_cron = True
                    cron_task, cron_body = parsed
            if is_cron and self._cardkit_streaming_available():
                try:
                    from datetime import datetime as _dt
                    if self._cardkit is None:
                        self._cardkit = CardKitStreamManager(self)
                    card_json = build_cron_card(
                        cron_task or "定时任务", cron_body,
                        job_name=cron_task,
                        timestamp=_dt.now().strftime("%m-%d %H:%M"),
                    )
                    return await self._cardkit.send_static_card(chat_id, card_json, metadata=metadata)
                except Exception as exc:
                    logger.warning("[CardKit] cron card send failed, falling back to text: %s", exc)
                    # fall through to the inherited text send

            # Quiet mode (default, CEO 2026-09-25): Feishu = task in → result out.
            # Tool records, status heartbeats and interim advisories are swallowed
            # BEFORE they ever hit the chat — no bubbles, no "(已编辑)" panels.
            # One card per turn is opened EARLY (first tool progress) with a grey
            # "⏳ 处理中…" status line (CEO option 2) so long quiet turns don't read
            # as stuck; the status disappears when the completed card renders.
            if _QUIET_DEFAULT:
                state_q = self._live_card_for_chat(chat_id)
                if state_q is not None:
                    # the turn is alive: a pending finalize must not fire mid-work
                    self._cancel_pending_finalize(state_q.message_id)
                if md.get("tool_progress"):
                    if state_q is None:
                        # open the turn card NOW (first sign of work) with the status line
                        try:
                            if self._cardkit is None:
                                self._cardkit = CardKitStreamManager(self)
                            state_q = await self._cardkit.create_and_send_streaming_card(
                                chat_id, reply_to=reply_to, metadata=metadata,
                                status_text="⏳ 处理中…")
                            self._cardkit_states[state_q.message_id] = state_q
                            while len(self._cardkit_states) > 32:
                                self._cardkit_states.pop(next(iter(self._cardkit_states)), None)
                        except Exception as exc:
                            logger.warning("[CardKit] quiet-mode early card open failed: %s", exc)
                            self._cardkit_streaming_on = False
                    elif _TOOL_PANEL_ON:
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

        # ── clarify buttons (CEO 2026-09-26): native choice card ──────────

        async def send_clarify(
            self, chat_id: str, question: str, choices: Optional[list], clarify_id: str,
            session_key: str, metadata: Optional[Dict[str, Any]] = None,
        ) -> SendResult:
            """Multiple-choice clarify as a CardKit button card; open-ended falls back.

            Clicks resolve via the card-action path (``hermes_clarify`` value) which
            calls ``resolve_gateway_clarify``; the card then swaps to the picked
            choice. Fails soft to the inherited numbered-list text prompt.
            """
            if choices and self._cardkit_streaming_available():
                try:
                    if self._cardkit is None:
                        self._cardkit = CardKitStreamManager(self)
                    card_json = build_clarify_card(question, choices, clarify_id=clarify_id)
                    result = await self._cardkit.send_static_card(chat_id, card_json, metadata=metadata)
                    if result.success and result.message_id:
                        self._clarify_cards[clarify_id] = {
                            "card_id": self._clarify_card_ids.get(result.message_id),
                            "message_id": result.message_id,
                            "question": question, "choices": list(choices),
                        }
                    return result
                except Exception as exc:
                    logger.warning("[CardKit] clarify card failed, falling back to text: %s", exc)
            return await super().send_clarify(
                chat_id, question, choices, clarify_id, session_key, metadata=metadata)

        def _on_card_action_trigger(self, data: Any) -> Any:
            """Intercept ``hermes_clarify`` button clicks BEFORE the built-in routing.

            Resolve the clarify, swap the card to the picked choice (buttons removed),
            and return an inline card response so all clients sync. Everything else
            falls through to the built-in handler (approval buttons, /card commands).
            """
            try:
                event = getattr(data, "event", None)
                action = getattr(event, "action", None)
                value = getattr(action, "value", {}) or {}
                payload = value.get("hermes_clarify") if isinstance(value, dict) else None
                if not payload:
                    return super()._on_card_action_trigger(data)
                clarify_id = str(payload.get("clarify_id") or "")
                choice = str(payload.get("choice") or "")
                if not clarify_id or not choice:
                    return super()._on_card_action_trigger(data)

                from tools.clarify_gateway import resolve_gateway_clarify
                resolved = resolve_gateway_clarify(clarify_id, choice)
                info = self._clarify_cards.pop(clarify_id, None)
                loop = self._loop
                if loop is not None and self._loop_accepts_callbacks(loop):
                    question = info["question"] if info else ""
                    card_id = (info or {}).get("card_id") or ""
                    async def _swap() -> None:
                        try:
                            if card_id:
                                if self._cardkit is None:
                                    self._cardkit = CardKitStreamManager(self)
                                answered = {
                                    "schema": "2.0",
                                    "config": {"streaming_mode": False, "locales": ["zh_cn", "en_us"],
                                               "update_multi": True,
                                               "summary": {"content": "Choice made",
                                                           "i18n_content": {"zh_cn": "已选择",
                                                                            "en_us": "Choice made"}}},
                                    "body": {"elements": [
                                        {"tag": "markdown", "content": question or "（问题已过期）",
                                         "text_align": "left", "text_size": "normal_v2"},
                                        {"tag": "markdown",
                                         "content": (f"✅ 已选择：**{choice}**" if resolved
                                                     else f"⚠️ 已选 {choice}（原问题已过期或已回答）"),
                                         "text_size": "notation"},
                                    ]},
                                }
                                await self._cardkit.update_card(card_id, answered, sequence=2)
                        except Exception as exc:
                            logger.debug("[CardKit] clarify card swap failed: %s", exc)
                    from agent.async_utils import safe_schedule_threadsafe
                    safe_schedule_threadsafe(
                        _swap(), loop, logger=logger,
                        log_message="[CardKit] clarify swap scheduling failed")
                # toast: immediate click feedback in the client (avoids the "no response" feel)
                resp = self._card_response()
                try:
                    resp.toast = {"type": "info", "content": f"已选择：{choice}"[:40]}
                except Exception:
                    pass
                return resp
            except Exception as exc:
                logger.warning("[feishu-cardkit] clarify click handling failed: %s", exc)
                return super()._on_card_action_trigger(data)


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
