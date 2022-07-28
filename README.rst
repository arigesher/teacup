# A local network Tempest client

This code will decode messages from a Tempest weather station.

Currently, it just spits out JSON records, one-per-line for
four types of messages:
* rapid wind
* station observation
* sky observation
* air observation

See https://weatherflow.github.io/Tempest/api/udp/v143/ for API details


## Install

1. install [poetry](https://python-poetry.org/docs/)
2. On macOS, use brew to install Python 3.10: `brew install python@3.10`
3. Use that installation (it's not linked into the path): `poetry env use /opt/homebrew/opt/python@3.10/bin/python3`
2. run `poetry install`

## Running

```
poetry run python -m teacup
```

