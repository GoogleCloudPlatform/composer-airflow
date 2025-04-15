from __future__ import annotations

from unit.composer.patches.core.test_data.cross_composer_patches_method import hook


def method_a(prefix):
    hook(f"{prefix} patch bear")
