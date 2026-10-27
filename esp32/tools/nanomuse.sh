#!/usr/bin/env bash
# CoreS3 LAN nanoMuse build, isolated from the official Muse profile.
set -euo pipefail
project_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
# Finding idf.py on PATH does not mean its Python environment is active:
# the gateway venv and another IDF export can both shadow the right tools.
is_supported_idf() {
    local version_file="$1/tools/cmake/version.cmake"
    [[ -f "$1/export.sh" && -f "$version_file" ]] &&
        grep -Eq '^set\(IDF_VERSION_MAJOR[[:space:]]+6\)' "$version_file" &&
        grep -Eq '^set\(IDF_VERSION_MINOR[[:space:]]+0\)' "$version_file" &&
        grep -Eq '^set\(IDF_VERSION_PATCH[[:space:]]+1\)' "$version_file"
}
idf_export=${IDF_EXPORT:-}
if [[ -z "$idf_export" ]]; then
    for idf_candidate in "${IDF_PATH:-}" "$HOME/esp/esp-idf-v6.0.1" \
        "$HOME/esp/esp-idf-v6" "$HOME/.espressif/esp-idf-v6.0.1" "$HOME/esp/esp-idf"; do
        if [[ -n "$idf_candidate" ]] && is_supported_idf "$idf_candidate"; then
            idf_export="$idf_candidate/export.sh"
            break
        fi
    done
fi
if [[ -z "$idf_export" ]] || ! is_supported_idf "$(dirname -- "$idf_export")"; then
    echo 'ESP-IDF v6.0.1 is required. Set IDF_EXPORT=/path/to/esp-idf-v6.0.1/export.sh.' >&2
    exit 1
fi
# Let the selected SDK discover its own managed Python environment. These
# changes are confined to this script, leaving the caller's gateway venv alone.
unset IDF_PYTHON_ENV_PATH PYTHONHOME PYTHONPATH
source "$idf_export" >/dev/null
if [[ ! -x "${IDF_PYTHON_ENV_PATH:-}/bin/python" ]]; then
    echo 'ESP-IDF Python activation failed; check the selected SDK installation.' >&2
    exit 1
fi
cd -- "$project_dir"
if [[ $# == 0 ]]; then set -- build; fi
exec "$IDF_PYTHON_ENV_PATH/bin/python" "$IDF_PATH/tools/idf.py" \
    -B build-nanomuse-cores3 -DIDF_TARGET=esp32s3 \
    -DSDKCONFIG=build-nanomuse-cores3/sdkconfig \
    '-DSDKCONFIG_DEFAULTS=sdkconfig.defaults;devices/sdkconfig.muse;devices/sdkconfig.muse-m5stack-cores3;sdkconfig.nanomuse' "$@"
