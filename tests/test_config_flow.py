"""Verify YAML import into the CozyLife Home Assistant config entry."""

from __future__ import annotations

from importlib.util import find_spec
import json
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import AsyncMock, Mock, patch

from homeassistant import loader
from homeassistant.config_entries import (
    ConfigEntries,
    SOURCE_IMPORT,
    SOURCE_USER,
)
from homeassistant.core import HomeAssistant
from homeassistant.loader import DATA_COMPONENTS

import custom_components.hass_cozylife_local_pull.config_flow as config_flow_module
from custom_components.hass_cozylife_local_pull.const import DOMAIN


CONFIG_FLOW_MODULE = "custom_components.hass_cozylife_local_pull.config_flow"
STRINGS_PATH = (
    Path(__file__).parents[1]
    / "custom_components"
    / "hass_cozylife_local_pull"
    / "strings.json"
)


class ConfigFlowContractTest(unittest.TestCase):
    """Define the integration's YAML-only config flow contract."""

    def test_config_flow_class_exists(self) -> None:
        """The integration exposes a Home Assistant config flow handler."""
        module_spec = find_spec(CONFIG_FLOW_MODULE)
        self.assertIsNotNone(module_spec)
        if module_spec is None:
            return

        module = __import__(CONFIG_FLOW_MODULE, fromlist=["CozyLifeConfigFlow"])
        self.assertTrue(hasattr(module, "CozyLifeConfigFlow"))

    def test_yaml_only_abort_has_user_facing_text(self) -> None:
        """The manual configuration abort explains where configuration lives."""
        self.assertTrue(STRINGS_PATH.exists())
        if not STRINGS_PATH.exists():
            return

        strings = json.loads(STRINGS_PATH.read_text(encoding="utf-8"))
        self.assertEqual(
            strings["config"]["abort"]["yaml_only"],
            "This integration is configured in configuration.yaml.",
        )


class ConfigFlowBehaviorTest(unittest.IsolatedAsyncioTestCase):
    """Verify YAML is the only source for the singleton config entry."""

    async def asyncSetUp(self) -> None:
        """Create a real Home Assistant config flow manager without disk writes."""
        self.config_dir = TemporaryDirectory()
        self.addCleanup(self.config_dir.cleanup)
        self.hass = HomeAssistant(self.config_dir.name)
        loader.async_setup(self.hass)
        self.entries = ConfigEntries(self.hass, {})
        self.hass.config_entries = self.entries
        self.hass.data[DATA_COMPONENTS][f"{DOMAIN}.config_flow"] = (
            config_flow_module
        )
        self.setup_patch = patch.object(
            self.entries,
            "async_setup",
            new=AsyncMock(return_value=True),
        )
        self.save_patch = patch.object(
            self.entries,
            "_async_schedule_save",
            new=Mock(),
        )
        self.setup_patch.start()
        self.save_patch.start()
        self.addCleanup(self.setup_patch.stop)
        self.addCleanup(self.save_patch.stop)

    async def asyncTearDown(self) -> None:
        """Cancel any config flows retained by Home Assistant."""
        self.entries.flow.async_shutdown()

    async def test_import_creates_one_config_entry_with_yaml_data(self) -> None:
        """The first YAML import persists data and a second import is rejected."""
        data = {"lang": "zh", "ip": ["192.0.2.10"]}

        result = await self.entries.flow.async_init(
            DOMAIN,
            context={"source": SOURCE_IMPORT},
            data=data,
        )
        duplicate_result = await self.entries.flow.async_init(
            DOMAIN,
            context={"source": SOURCE_IMPORT},
            data={"lang": "en", "ip": []},
        )

        self.assertEqual(result["type"], "create_entry")
        entry = result["result"]
        self.assertEqual(entry.domain, DOMAIN)
        self.assertEqual(entry.source, SOURCE_IMPORT)
        self.assertEqual(entry.title, "CozyLife local")
        self.assertEqual(entry.data, data)
        self.assertEqual(self.entries.async_entries(DOMAIN), [entry])
        self.assertEqual(duplicate_result["type"], "abort")
        self.assertEqual(duplicate_result["reason"], "already_configured")

    async def test_user_step_directs_configuration_to_yaml(self) -> None:
        """The integration does not expose a second configuration surface."""
        result = await self.entries.flow.async_init(
            DOMAIN,
            context={"source": SOURCE_USER},
        )

        self.assertEqual(result["type"], "abort")
        self.assertEqual(result["reason"], "yaml_only")
