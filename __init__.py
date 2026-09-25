"""hermes-feishu-cardkit — CardKit streaming cards for the Feishu/Lark channel.

A drop-in platform plugin that registers over the built-in ``feishu`` platform
(last-writer-wins registry) with an adapter subclassing the built-in
FeishuAdapter. All base behaviour (WebSocket, threads, media, approvals,
pairing, comment events) is inherited; this subclass only adds:

  1. ``send()``     — streaming FIRST frame (``expect_edits``) opens a CardKit
                      2.0 card bubble (create card entity + interactive message).
  2. ``edit_message()`` — later frames stream into the card via
                      ``cardElement.content``; ``finalize=True`` swaps in the
                      completed card (collapsible 🛠️ tool panel + footer) and
                      closes ``streaming_mode``.
  3. Tool-progress folding — progress sends carrying ``tool_progress`` metadata
                      are swallowed into the card's tool panel instead of
                      separate bubbles.

Fail-safe by design: any CardKit API failure latches the card path off for the
process and the adapter degrades to the inherited text/post edit streaming —
replies are never lost. Set ``FEISHU_CARDKIT_STREAMING=0`` to disable outright.
"""

from __future__ import annotations

import contextlib
import logging
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
from .cardkit_stream import CardKitStreamManager, cardkit_enabled, cardkit_sdk_available

_CURSOR = " ▉"


def _cardkit_available() -> bool:
    return cardkit_enabled() and cardkit_sdk_available()


if _BUILTIN_AVAILABLE:

    class FeishuCardKitAdapter(FeishuAdapter):
        """Built-in FeishuAdapter + CardKit streaming cards."""

        # The card has a distinct "processing" state that MUST be closed by an
        # explicit finalize edit even when the visible text is unchanged (same
        # contract as DingTalk AI Cards).
        REQUIRES_EDIT_FINALIZE = True

        def __init__(self, config: PlatformConfig):
            super().__init__(config)
            self._cardkit_states: Dict[str, Any] = {}
            self._cardkit: Optional[CardKitStreamManager] = None
            self._cardkit_streaming_on = True  # process-lifetime latch; False after a hard failure

        # ── helpers ────────────────────────────────────────────────────────

        def _cardkit_streaming_available(self) -> bool:
            if self._cardkit_streaming_on is False:
                return False
            return _cardkit_available()

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

        # ── streaming lifecycle ────────────────────────────────────────────

        async def send(
            self, chat_id: str, content: str, reply_to: Optional[str] = None,
            metadata: Optional[Dict[str, Any]] = None,
        ) -> SendResult:
            if not self._client:
                return SendResult(success=False, error="Not connected")

            # Tool-progress lines while a CardKit card is live in this chat: fold
            # them into the card's collapsible tool panel instead of separate bubbles.
            if (metadata or {}).get("tool_progress") and self._cardkit_states:
                for state in reversed(list(self._cardkit_states.values())):
                    if state.chat_id == chat_id and not state.finalized:
                        state.tool_lines.append(content.strip())
                        if len(state.tool_lines) > 40:
                            state.tool_lines = state.tool_lines[-40:]
                        return SendResult(success=True, message_id=state.message_id)

            # Streaming FIRST frame → open a CardKit card bubble.
            if (metadata or {}).get("expect_edits") and self._cardkit_streaming_available():
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
                        # REQUIRES_EDIT_FINALIZE routes a second finalize edit here;
                        # the card is already sealed — idempotent success, no API call.
                        return SendResult(success=True, message_id=message_id)
                    clean = self._cardkit.strip_cursor(content, _CURSOR)
                    if finalize:
                        # Segment-break finalizes also arrive with finalize=True; card
                        # semantics: finalize seals THIS card, the next segment's first
                        # send opens a fresh card (seal-and-continue).
                        await self._cardkit.finalize_card(state, clean, model=self._last_model_hint())
                        state.finalized = True
                    elif clean and clean != state.last_pushed:
                        await self._cardkit.stream_content(state, clean)
                    return SendResult(success=True, message_id=message_id)
                except Exception as exc:
                    logger.warning("[CardKit] streaming update failed, falling back to text edit: %s", exc)
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
