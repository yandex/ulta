#!/bin/bash

set -x -euo pipefail

# ARGS: a list of packages to be installed

ULTA_PYTHON=${ULTA_PYTHON:-"python3"}
ULTA_VENV=${ULTA_VENV:-"/opt/ulta/venv"}

EXECUTABLE="/usr/local/bin/ulta"
if [[ -f "$EXECUTABLE" ]]; then
    echo "$EXECUTABLE is already installed" && exit 1
fi

"$ULTA_PYTHON" -m venv "$ULTA_VENV"
(
    . "${ULTA_VENV}/bin/activate"

    # setuptools cap 71.0.0: https://github.com/pypa/setuptools/issues/4483
    python3 -m ensurepip --upgrade && python3 -m pip install --upgrade \
        "pip" \
        "setuptools>=51.0.0,<71.0.0" \
        "$@"
)

touch "$EXECUTABLE" && chmod 755 "$EXECUTABLE"

# ULTA_VENV expands at build time, @ expands at runtime
cat <<EOF >>"$EXECUTABLE"
#!/bin/bash
PATH="${ULTA_VENV}/bin:${PATH}" ulta "\$@"
EOF
