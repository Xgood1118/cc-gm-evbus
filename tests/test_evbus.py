# -*- coding: utf-8 -*-
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import evbus as mod  # noqa: E402


def test_priority_order():
    b = evbus.Bus()
    seen = []
    evbus.subscribe(b, "t", lambda p: seen.append("low"), 1)
    evbus.subscribe(b, "t", lambda p: seen.append("high"), 9)
    evbus.publish(b, "t", {})
    evbus.dispatch(b, 4)
    assert seen[:2] == ["high", "low"]


def test_unsubscribe_self():
    b = evbus.Bus()
    evbus.subscribe(b, "t", lambda p: None, 1)
    evbus.unsubscribe(b, "t", 1)
    assert b.handlers.get("t", []) == []
