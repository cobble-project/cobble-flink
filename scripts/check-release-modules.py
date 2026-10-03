#!/usr/bin/env python3
"""Check the release lists against this repository's explicit POM dependencies."""

import os
from pathlib import Path
import re
import sys
import xml.etree.ElementTree as ET

NS = {"m": "http://maven.apache.org/POM/4.0.0"}
PROPERTY = re.compile(r"\$\{([^}]+)\}")


def properties(pom):
    return {
        node.tag.split("}")[-1]: (node.text or "").strip()
        for node in pom.findall("m:properties/*", NS)
    }


def resolve(value, props):
    for _ in range(len(props) + 1):
        if not PROPERTY.search(value):
            return value
        value = PROPERTY.sub(lambda match: props[match[1]], value)
    raise ValueError(f"Cannot resolve POM property in {value}")


def coordinates(node, props, parent=None):
    values = []
    for name in ("groupId", "artifactId", "version"):
        value = node.findtext(f"m:{name}", namespaces=NS)
        if value is None and parent is not None and name != "artifactId":
            value = parent.findtext(f"m:{name}", namespaces=NS)
        if value is None:
            raise ValueError(f"Missing {name} in POM coordinates")
        values.append(resolve(value.strip(), props))
    return tuple(values)


def check_release_modules(root, release, core):
    root_pom = ET.parse(root / "pom.xml").getroot()
    root_props = properties(root_pom)
    modules = ["."] + [
        node.text.strip() for node in root_pom.findall("m:modules/m:module", NS)
    ]
    projects = {}
    references = {}
    for module in modules:
        pom = ET.parse(root / module / "pom.xml").getroot()
        props = {**root_props, **properties(pom)}
        parent = pom.find("m:parent", NS)
        gav = coordinates(pom, props, parent)
        if gav in projects:
            raise ValueError(f"Duplicate reactor coordinates: {gav}")
        projects[gav] = module
        nodes = pom.findall("m:dependencies/m:dependency", NS)
        if parent is not None:
            nodes.append(parent)
        references[module] = [coordinates(node, props) for node in nodes]

    unknown = (release | core) - set(modules)
    if unknown:
        raise ValueError(f"Unknown release modules: {sorted(unknown)}")
    if not core <= release:
        raise ValueError(f"Core modules missing from RELEASE_MODULES: {sorted(core - release)}")

    local_artifacts = {gav[:2] for gav in projects}
    errors = []
    for module in sorted(release):
        for gav in references[module]:
            if gav[:2] not in local_artifacts:
                continue
            dependency = projects.get(gav)
            if dependency is None:
                errors.append(f"{module}: no reactor module matches {':'.join(gav)}")
                continue
            # Every local dependency (including the parent) must publish in the
            # core batch before the separately deployed dist/monitor artifacts.
            for label, selected in (("RELEASE_MODULES", release), ("CORE_RELEASE_MODULES", core)):
                if dependency not in selected:
                    errors.append(f"{module}: {dependency} ({':'.join(gav)}) missing from {label}")
    if errors:
        raise ValueError("Release dependency closure failed:\n" + "\n".join(errors))


if __name__ == "__main__":
    try:
        check_release_modules(
            Path.cwd(),
            set(os.environ["RELEASE_MODULES"].split(",")),
            set(os.environ["CORE_RELEASE_MODULES"].split(",")),
        )
    except (ValueError, KeyError) as error:
        sys.exit(str(error))
    print("Release dependency closure passed (including local parents).")
