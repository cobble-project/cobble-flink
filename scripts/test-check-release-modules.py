import importlib.util
from pathlib import Path
import unittest

ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location(
    "check_release_modules", ROOT / "scripts/check-release-modules.py"
)
CHECK = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(CHECK)


class ReleaseModulesTest(unittest.TestCase):
    def setUp(self):
        workflow = (ROOT / ".github/workflows/maven-central.yml").read_text()
        lists = {}
        for line in workflow.splitlines():
            key, separator, value = line.strip().partition(": ")
            if separator and key in ("RELEASE_MODULES", "CORE_RELEASE_MODULES"):
                lists[key] = set(value.split(","))
        self.release = lists["RELEASE_MODULES"]
        self.core = lists["CORE_RELEASE_MODULES"]

    def test_workflow_lists_pass(self):
        CHECK.check_release_modules(ROOT, self.release, self.core)

    def test_missing_compaction_versions_fail(self):
        for module in ("cobble-dedicated-compaction", "cobble-dedicated-compaction-flink-2.0"):
            with self.subTest(module=module):
                with self.assertRaisesRegex(ValueError, f"{module} .*missing from RELEASE_MODULES"):
                    CHECK.check_release_modules(ROOT, self.release - {module}, self.core - {module})

    def test_missing_core_batch_dependency_fails(self):
        with self.assertRaisesRegex(ValueError, "cobble-dedicated-compaction .*missing from CORE_RELEASE_MODULES"):
            CHECK.check_release_modules(ROOT, self.release, self.core - {"cobble-dedicated-compaction"})

    def test_missing_parent_fails(self):
        with self.assertRaisesRegex(ValueError, r": \. .*missing from RELEASE_MODULES"):
            CHECK.check_release_modules(ROOT, self.release - {"."}, self.core - {"."})

    def test_external_cobble_artifact_is_not_required(self):
        CHECK.check_release_modules(ROOT, {".", "cobble-common"}, {".", "cobble-common"})


if __name__ == "__main__":
    unittest.main()
