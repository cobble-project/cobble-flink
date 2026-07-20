#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
MAVEN_REPOSITORY="${MAVEN_REPOSITORY:-${HOME}/.m2/repository}"
REQUIRED_VERSIONS=("1.17.2" "1.19.3" "2.0.0")

missing=()
for version in "${REQUIRED_VERSIONS[@]}"; do
  core_jar="${MAVEN_REPOSITORY}/org/apache/flink/flink-core/${version}/flink-core-${version}.jar"
  [[ -f "${core_jar}" ]] || missing+=("${core_jar}")

  if [[ "${version}" == 2.* ]]; then
    core_api_jar="${MAVEN_REPOSITORY}/org/apache/flink/flink-core-api/${version}/flink-core-api-${version}.jar"
    [[ -f "${core_api_jar}" ]] || missing+=("${core_api_jar}")
  fi
done

if (( ${#missing[@]} > 0 )); then
  printf 'Required Flink jars are missing from the local Maven repository:\n' >&2
  printf '  %s\n' "${missing[@]}" >&2
  printf 'Resolve the monitor test dependencies before running this validation.\n' >&2
  exit 1
fi

java_bin="${JAVA_HOME:+${JAVA_HOME}/bin/}java"
if ! command -v "${java_bin}" >/dev/null 2>&1; then
  printf 'Java is not available. Set JAVA_HOME to a JDK 11 installation.\n' >&2
  exit 1
fi

java_version="$("${java_bin}" -version 2>&1 | sed -n '1s/.*version "\([^"]*\)".*/\1/p')"
if [[ "${java_version}" != 11.* ]]; then
  printf 'This validation requires JDK 11; found %s.\n' "${java_version:-unknown}" >&2
  exit 1
fi

if [[ -n "${JAVA_HOME:-}" ]]; then
  export PATH="${JAVA_HOME}/bin:${PATH}"
fi

cd "${PROJECT_ROOT}"
./mvnw --batch-mode --no-transfer-progress \
  -pl cobble-flink-monitor -am \
  -Dtest=CrossVersionPojoProtocolValidation \
  -Dcross.version.maven.repo="${MAVEN_REPOSITORY}" \
  -Dsurefire.failIfNoSpecifiedTests=false \
  test
