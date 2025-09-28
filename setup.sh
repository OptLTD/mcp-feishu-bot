rm -rf dist/*
uv pip install build twine
uv run python -m build
uv run twine upload dist/*