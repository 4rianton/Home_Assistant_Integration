from __future__ import annotations

import logging
from functools import partial
from datetime import datetime, timedelta, timezone
from typing import Any

import requests
from homeassistant.core import HomeAssistant, ServiceCall
from homeassistant.helpers.dispatcher import async_dispatcher_connect
from homeassistant.helpers.event import async_track_time_interval

from .const import DOMAIN, SIGNAL_AP_UPDATE
from .util import get_hub_from_hass

_LOGGER = logging.getLogger(__name__)

_STATE: dict[str, Any] = {"last_sync": None, "last_seen": None, "unsub": None, "cancel": None}

_STALE_THRESHOLD = timedelta(minutes=20)
_STALE_CHECK_INTERVAL = timedelta(minutes=5)


async def _do_sync(hass: HomeAssistant, state: dict[str, Any], hub=None) -> bool:
    """Post current UTC epoch to the AP /set_time endpoint.

    Returns True on HTTP 200, False otherwise.
    """
    if hub is None:
        try:
            hub = get_hub_from_hass(hass)
        except Exception as err:
            _LOGGER.error("Time sync failed: %s", err)
            return False

    if not getattr(hub, "online", False):
        _LOGGER.error("Cannot sync time: AP is offline")
        return False

    epoch = int(datetime.now(timezone.utc).timestamp())

    def _post():
        return requests.post(f"http://{hub.host}/set_time", data={"epoch": epoch}, timeout=5)

    try:
        response = await hass.async_add_executor_job(_post)
        if response.status_code == 200:
            state["last_sync"] = datetime.now()
            _LOGGER.info("Synchronized AP time to epoch %s", epoch)
            return True
        _LOGGER.error("Failed to sync AP time: HTTP %s", response.status_code)
        return False
    except Exception as err:
        _LOGGER.error("Failed to sync AP time: %s", err)
        return False


async def _maybe_auto_sync(hass: HomeAssistant, state: dict[str, Any]) -> None:
    """Auto-sync AP time if invalid or drifted beyond threshold, with cooldown."""
    try:
        hub = get_hub_from_hass(hass)
    except Exception:
        return

    if not getattr(hub, "online", False):
        return

    # Read AP time from hub's internal data if available
    ap_time = None
    try:
        ap_time = int(getattr(hub, "_ap_data", {}).get("sys_time"))
    except Exception:
        ap_time = None

    if ap_time is None:
        return

    state["last_seen"] = datetime.now()

    now_epoch = int(datetime.now(timezone.utc).timestamp())
    drift = abs(now_epoch - ap_time)
    needs_sync = (ap_time < 1600000000) or drift >= 2

    if not needs_sync:
        return

    last_sync = state.get("last_sync")
    if last_sync is not None and (datetime.now() - last_sync) <= timedelta(minutes=5):
        return

    _LOGGER.warning(
        "AP time out of sync by %ss (ap=%s, ha=%s). Syncing...",
        drift,
        ap_time,
        now_epoch,
    )
    await _do_sync(hass, state, hub)


async def _handle_ap_update(hass: HomeAssistant, state: dict[str, Any]) -> None:
    await _maybe_auto_sync(hass, state)


async def _check_stale_time(
    hass: HomeAssistant,
    state: dict[str, Any],
    _: datetime,
) -> None:
    """Force a sync if AP time hasn't been refreshed recently."""
    now = datetime.now()
    candidates = [dt for dt in (state.get("last_sync"), state.get("last_seen")) if dt is not None]
    last_activity = max(candidates) if candidates else None

    if last_activity is not None and (now - last_activity) < _STALE_THRESHOLD:
        return

    _LOGGER.warning("AP time stale for %s. Forcing synchronization.", _STALE_THRESHOLD)
    await _do_sync(hass, state)


async def async_setup(hass: HomeAssistant) -> None:
    """Initialize time sync extension: service and AP update listener."""
    state = _STATE

    async def handle_service(call: ServiceCall) -> None:
        ok = await _do_sync(hass, state)
        title = "OEPL AP Time Sync"
        message = "AP time synchronized from Home Assistant." if ok else "Failed to synchronize AP time. Check logs."
        try:
            await hass.services.async_call(
                "persistent_notification",
                "create",
                {
                    "title": title,
                    "message": message,
                    "notification_id": "oepl_ap_time_sync",
                },
            )
        except Exception:
            # Notification is best-effort; ignore if platform not loaded yet
            pass

    # Register service to sync time on demand
    if not hass.services.has_service(DOMAIN, "sync_ap_time"):
        hass.services.async_register(DOMAIN, "sync_ap_time", handle_service)

    # Subscribe to AP updates for automatic sync checks
    if state.get("unsub") is None:
        state["unsub"] = async_dispatcher_connect(
            hass,
            SIGNAL_AP_UPDATE,
            partial(_handle_ap_update, hass, state),
        )

    if state.get("cancel") is None:
        state["cancel"] = async_track_time_interval(
            hass,
            partial(_check_stale_time, hass, state),
            _STALE_CHECK_INTERVAL,
        )
