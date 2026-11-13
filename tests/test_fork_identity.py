import re
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent


def test_updater_points_at_fork():
    src = (ROOT / "gui/main_window.py").read_text()
    assert '"cognitiveghost/packing-tool-db"' in src
    assert '"cognitiveghost/packing-tool"' not in src


def test_no_production_qsettings_org():
    pat = re.compile(r'QSettings\("(PackingTool)"')
    hits = [
        str(p)
        for p in ROOT.rglob("*.py")
        if ".venv" not in p.parts and pat.search(p.read_text(encoding="utf-8"))
    ]
    assert hits == []
