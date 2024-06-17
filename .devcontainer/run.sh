
if [[ -d .venv ]]; then
    uv venv
fi

pip uninstall playwright

source .venv/bin/activate


uv pip install -r requirements.in
uv pip install -e .

playwright install
playwright install-de