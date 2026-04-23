#!/usr/bin/env bash

set -euo pipefail

if [[ $# -ne 1 ]]; then
  echo "Usage: $0 <path-to-cobble-java/java>" >&2
  exit 1
fi

COBBLE_JAVA_DIR="$(cd "$1" && pwd -P)"
MVN_CMD="${MVN_CMD:-mvn}"
PYTHON_CMD="${PYTHON_CMD:-}"

if [[ ! -f "${COBBLE_JAVA_DIR}/pom.xml" ]]; then
  echo "Missing pom.xml under ${COBBLE_JAVA_DIR}" >&2
  exit 1
fi

pushd "${COBBLE_JAVA_DIR}" >/dev/null

if [[ -z "${PYTHON_CMD}" ]]; then
  if command -v python3 >/dev/null 2>&1; then
    PYTHON_CMD="python3"
  elif command -v python >/dev/null 2>&1; then
    PYTHON_CMD="python"
  else
    echo "Neither python3 nor python is available" >&2
    exit 1
  fi
fi

SOURCE_COORDS="$(
  "${PYTHON_CMD}" - "${COBBLE_JAVA_DIR}/pom.xml" <<'PY'
import sys
import xml.etree.ElementTree as ET

source_pom = sys.argv[1]
namespace = "http://maven.apache.org/POM/4.0.0"
root = ET.parse(source_pom).getroot()

def text_or_none(element_name):
    element = root.find(f"{{{namespace}}}{element_name}")
    return element.text if element is not None else None

artifact_id = text_or_none("artifactId")
version = text_or_none("version")

if version is None:
    parent = root.find(f"{{{namespace}}}parent")
    if parent is not None:
        version_element = parent.find(f"{{{namespace}}}version")
        version = version_element.text if version_element is not None else None

if artifact_id is None or version is None:
    raise SystemExit("Unable to resolve source artifact coordinates from pom.xml")

print(artifact_id)
print(version)
PY
)"

SOURCE_ARTIFACT_ID="$(printf '%s\n' "${SOURCE_COORDS}" | sed -n '1p')"
SOURCE_VERSION="$(printf '%s\n' "${SOURCE_COORDS}" | sed -n '2p')"
TARGET_ARTIFACT_ID="${COBBLE_JAVA_ARTIFACT_ID:-${SOURCE_ARTIFACT_ID}}"

"${MVN_CMD}" --batch-mode --no-transfer-progress -DskipTests -Dspotless.check.skip=true clean package

JAR_PATH="target/${SOURCE_ARTIFACT_ID}-${SOURCE_VERSION}.jar"
if [[ ! -f "${JAR_PATH}" ]]; then
  echo "Expected jar not found: ${COBBLE_JAVA_DIR}/${JAR_PATH}" >&2
  exit 1
fi

INSTALL_POM="${COBBLE_JAVA_DIR}/pom.xml"
if [[ "${TARGET_ARTIFACT_ID}" != "${SOURCE_ARTIFACT_ID}" ]]; then
  TMP_POM="$(mktemp)"
  trap 'rm -f "${TMP_POM}"' EXIT

  "${PYTHON_CMD}" - "${COBBLE_JAVA_DIR}/pom.xml" "${TMP_POM}" "${TARGET_ARTIFACT_ID}" <<'PY'
import sys
import xml.etree.ElementTree as ET

source_pom, target_pom, artifact_id = sys.argv[1:4]
namespace = "http://maven.apache.org/POM/4.0.0"
ET.register_namespace("", namespace)
root = ET.parse(source_pom).getroot()
artifact = root.find(f"{{{namespace}}}artifactId")
if artifact is None:
    raise SystemExit("artifactId not found in source pom")
artifact.text = artifact_id
ET.ElementTree(root).write(target_pom, encoding="utf-8", xml_declaration=True)
PY
  INSTALL_POM="${TMP_POM}"
fi

"${MVN_CMD}" --batch-mode --no-transfer-progress org.apache.maven.plugins:maven-install-plugin:3.1.2:install-file \
  -Dfile="${JAR_PATH}" \
  -DpomFile="${INSTALL_POM}"

popd >/dev/null
