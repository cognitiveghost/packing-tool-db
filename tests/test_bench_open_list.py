"""The open-list benchmark never touches the hosted project (scripts/bench_open_list.py)."""

import pytest

from scripts.bench_open_list import require_local


def test_refuses_hosted_or_missing_url():
    with pytest.raises(SystemExit):
        require_local("https://ntzqarcksthplcdzivep.supabase.co")
    with pytest.raises(SystemExit):
        require_local(None)


def test_accepts_local_stack_url():
    assert require_local("http://127.0.0.1:54321") == "http://127.0.0.1:54321"
    assert require_local("http://localhost:54321") == "http://localhost:54321"
