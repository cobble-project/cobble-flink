#!/usr/bin/env python3
"""Validate repository-local Codex skills without external dependencies."""

from pathlib import Path
import re
import sys


NAME_PATTERN = re.compile(r"^[a-z0-9]+(?:-[a-z0-9]+)*$")


def parse_frontmatter(skill_file: Path) -> dict[str, str]:
    lines = skill_file.read_text(encoding="utf-8").splitlines()
    if not lines or lines[0] != "---":
        raise ValueError("SKILL.md must start with YAML frontmatter")
    try:
        end = lines.index("---", 1)
    except ValueError as error:
        raise ValueError("SKILL.md frontmatter is not closed") from error

    values: dict[str, str] = {}
    for line in lines[1:end]:
        if not line.strip() or line.lstrip().startswith("#"):
            continue
        key, separator, value = line.partition(":")
        if not separator:
            raise ValueError(f"invalid frontmatter line: {line}")
        values[key.strip()] = value.strip().strip('"\'')
    return values


def validate(skill_dir: Path) -> None:
    skill_file = skill_dir / "SKILL.md"
    if not skill_file.is_file():
        raise ValueError(f"missing {skill_file}")

    metadata = parse_frontmatter(skill_file)
    name = metadata.get("name", "")
    description = metadata.get("description", "")
    if name != skill_dir.name:
        raise ValueError(f"skill name '{name}' must match directory '{skill_dir.name}'")
    if len(name) > 64 or not NAME_PATTERN.fullmatch(name):
        raise ValueError("skill name must be <=64 lowercase letters, digits, or hyphens")
    if not description or "TODO" in description:
        raise ValueError("skill description must be complete")
    if len(description) > 1024:
        raise ValueError("skill description must be <=1024 characters")

    interface = skill_dir / "agents" / "openai.yaml"
    if not interface.is_file():
        raise ValueError(f"missing {interface}")
    interface_text = interface.read_text(encoding="utf-8")
    for field in ("display_name:", "short_description:", "default_prompt:"):
        if field not in interface_text:
            raise ValueError(f"{interface} is missing {field[:-1]}")


def main() -> int:
    if len(sys.argv) != 2:
        print("usage: python3 skills/validate.py <skill-directory>", file=sys.stderr)
        return 2
    skill_dir = Path(sys.argv[1])
    try:
        validate(skill_dir)
    except (OSError, ValueError) as error:
        print(f"invalid skill: {error}", file=sys.stderr)
        return 1
    print(f"valid skill: {skill_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
