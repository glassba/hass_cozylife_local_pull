"""Validate metadata required for the supported Home Assistant releases."""

from __future__ import annotations

import json
from pathlib import Path
import unittest


MANIFEST_PATH = (
    Path(__file__).parents[1]
    / "custom_components"
    / "hass_cozylife_local_pull"
    / "manifest.json"
)


class IntegrationManifestTest(unittest.TestCase):
    """Verify the custom integration manifest metadata."""

    def setUp(self) -> None:
        """Load the production manifest for each assertion."""
        self.manifest = json.loads(MANIFEST_PATH.read_text(encoding="utf-8"))

    def test_integration_type_is_explicitly_hub(self) -> None:
        """The integration exposes multiple local devices as a hub."""
        self.assertEqual(self.manifest["integration_type"], "hub")

    def test_manifest_version_marks_compatibility_release(self) -> None:
        """The Home Assistant compatibility fix has a patch release version."""
        self.assertEqual(self.manifest["version"], "0.2.1")

    def test_yaml_import_enables_config_flow(self) -> None:
        """YAML devices load through a Home Assistant config entry."""
        self.assertTrue(self.manifest["config_flow"])
