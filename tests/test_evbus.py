# -*- coding: utf-8 -*-
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pytest  # noqa: E402

import evbus as evbus  # noqa: E402


def test_priority_order():
    b = evbus.Bus()
    seen = []
    evbus.subscribe(b, "t", lambda p: seen.append("low"), 1)
    evbus.subscribe(b, "t", lambda p: seen.append("high"), 9)
    evbus.publish(b, "t", {})
    evbus.dispatch(b, 4)
    assert seen[:2] == ["high", "low"]


def test_same_priority_follows_registration_order():
    b = evbus.Bus()
    seen = []
    evbus.subscribe(b, "t", lambda p: seen.append("a"), 5)
    evbus.subscribe(b, "t", lambda p: seen.append("b"), 5)
    evbus.subscribe(b, "t", lambda p: seen.append("c"), 5)
    evbus.publish(b, "t", {})
    evbus.dispatch(b, 4)
    assert seen == ["a", "b", "c"]


def test_subscription_during_dispatch_takes_effect_next_round():
    """本轮派发开始后新增的订阅不能影响本轮，第二轮才生效。"""
    b = evbus.Bus()
    seen = []

    def late_subscriber(payload):
        seen.append("late")

    def registrar(payload):
        seen.append("first")
        evbus.subscribe(b, "t", late_subscriber, 9)  # 高优先级也不能插队本轮

    evbus.subscribe(b, "t", registrar, 1)
    evbus.publish(b, "t", {})   # 第一轮
    evbus.publish(b, "t", {})   # 第二轮
    evbus.dispatch(b, 4)
    # 第一轮只有 registrar；第二轮 late（priority=9）先于 registrar 执行
    assert seen == ["first", "late", "first"]
    assert seen.count("late") == 1  # 高优先级也没能插进第一轮


def test_reentry_nesting_and_deferred_not_lost():
    """回调里 publish：受深度上限约束，超深事件延后但不丢弃。"""
    b = evbus.Bus()
    order = []

    def on_t(payload):
        order.append(("t", b.depth))
        evbus.publish(b, "m", {})

    def on_m(payload):
        order.append(("m", b.depth))
        evbus.publish(b, "n", {})  # 在深度 2 发布，limit=2 时必须延后

    def on_n(payload):
        order.append(("n", b.depth))

    evbus.subscribe(b, "t", on_t, 0)
    evbus.subscribe(b, "m", on_m, 0)
    evbus.subscribe(b, "n", on_n, 0)

    evbus.publish(b, "t", {})
    evbus.dispatch(b, 2)  # 顶层深度 1，嵌套只允许到深度 2

    # 三个事件都恰好派发一次，没有丢失
    assert b.dispatches == 3
    assert b.deferred == 1                          # n 在深度 2 触顶延后
    assert b.max_depth_seen == 2
    assert order == [("t", 1), ("m", 2), ("n", 1)]  # 延后事件在顶层补派
    assert b.depth == 0 and b.queue == []


def test_max_depth_is_configurable():
    """同一模式在不同 max_depth 下，深度与延后计数随配置变化。"""
    def run(limit):
        b = evbus.Bus()
        state = {"left": 3}

        def on_t(payload):
            if state["left"] > 0:  # 自终止的重入，避免无限事件链
                state["left"] -= 1
                evbus.publish(b, "t", {})

        evbus.subscribe(b, "t", on_t, 0)
        evbus.publish(b, "t", {})
        evbus.dispatch(b, limit)
        return b

    b3 = run(3)
    assert b3.max_depth_seen == 3
    assert b3.deferred == 1
    assert b3.queue == []

    b1 = run(1)
    assert b1.max_depth_seen == 1
    assert b1.deferred == 3
    assert b1.queue == []


def test_exception_isolation():
    """单个回调抛异常不影响同轮其他订阅者与后续事件。"""
    b = evbus.Bus()
    seen = []

    def boom(payload):
        raise ValueError("boom")

    evbus.subscribe(b, "t", boom, 9)
    evbus.subscribe(b, "t", lambda p: seen.append("ok"), 1)
    evbus.publish(b, "t", {})
    evbus.publish(b, "t", {})
    evbus.dispatch(b, 2)

    assert seen == ["ok", "ok"]
    assert b.errors == 2
    assert b.error_log[0][0] == "t"
    assert b.error_log[0][3] == "ValueError"


def test_publish_without_subscribers():
    """无订阅者：事件正常计为一次派发，总线状态干净。"""
    b = evbus.Bus()
    evbus.publish(b, "nobody", {"x": 1})
    evbus.dispatch(b, 2)
    assert b.dispatches == 1
    assert b.errors == 0
    assert b.queue == []


def test_unsubscribe_self_inside_callback():
    """回调退订自己：本轮执行完，后续轮次不再出现。"""
    b = evbus.Bus()
    calls = []

    def self_removing(payload):
        calls.append("self")
        assert evbus.unsubscribe(b, "t", 0, self_removing) == 1

    evbus.subscribe(b, "t", self_removing, 0)
    evbus.publish(b, "t", {})
    evbus.publish(b, "t", {})
    evbus.dispatch(b, 2)

    assert calls == ["self"]
    assert b.handlers.get("t", []) == []
    assert b.dispatches == 2


def test_unsubscribe_self():
    b = evbus.Bus()
    evbus.subscribe(b, "t", lambda p: None, 1)
    evbus.unsubscribe(b, "t", 1)
    assert b.handlers.get("t", []) == []


def test_repeated_dispatch_is_deterministic():
    """同一事件序列两次派发顺序一致。"""
    def run():
        b = evbus.Bus()
        seen = []
        evbus.subscribe(b, "t", lambda p: seen.append(("lo", b.depth)), 1)
        evbus.subscribe(b, "t", lambda p: seen.append(("hi", b.depth)), 9)

        def reentrant(payload):
            seen.append(("mid", b.depth))
            evbus.publish(b, "nested", {})

        evbus.subscribe(b, "t", reentrant, 5)
        evbus.subscribe(b, "nested", lambda p: seen.append(("n", b.depth)), 0)
        evbus.publish(b, "t", {})
        evbus.publish(b, "t", {})
        evbus.dispatch(b, 3)
        return seen, b

    seen1, b1 = run()
    seen2, b2 = run()
    assert seen1 == seen2
    assert (b1.dispatches, b1.deferred, b1.max_depth_seen) == \
           (b2.dispatches, b2.deferred, b2.max_depth_seen)


def test_invalid_max_depth():
    b = evbus.Bus()
    evbus.publish(b, "t", {})
    with pytest.raises(ValueError):
        evbus.dispatch(b, 0)
