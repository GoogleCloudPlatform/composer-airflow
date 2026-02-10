from __future__ import annotations

from airflow.composer.patches.core.utils import cross_composer_patches_method

from unit.composer.patches.core.test_data.cross_composer_patches_method import hook


@cross_composer_patches_method
def method_a(prefix):
    hook(f"{prefix} patch core")
