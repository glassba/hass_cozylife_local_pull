"""Import CozyLife YAML settings into a Home Assistant config entry."""

from __future__ import annotations

from typing import Any

from homeassistant import config_entries
from homeassistant.config_entries import ConfigFlowResult

from .const import DOMAIN


class CozyLifeConfigFlow(config_entries.ConfigFlow, domain=DOMAIN):
    """Manage the single YAML-backed CozyLife config entry."""

    VERSION = 1

    async def async_step_import(
        self, import_data: dict[str, Any]
    ) -> ConfigFlowResult:
        """Create the single config entry from YAML settings."""
        if self._async_current_entries():
            return self.async_abort(reason="already_configured")
        return self.async_create_entry(
            title="CozyLife local",
            data=import_data,
        )

    async def async_step_user(
        self, user_input: dict[str, Any] | None = None
    ) -> ConfigFlowResult:
        """Keep YAML as the integration's only configuration surface."""
        return self.async_abort(reason="yaml_only")
