# -*- coding: utf-8 -*-
"""事件总线：优先级派发、可重入、不丢事件。

语义约定（详见 README）：

- 同一 topic 的订阅者按 ``priority`` 数值降序执行，同优先级按注册顺序执行。
- 每次事件派发开始时对订阅列表做快照，派发途中 subscribe/unsubscribe
  不影响本轮派发，下一轮才生效。
- 回调中 publish：未超过 ``max_depth`` 时就地嵌套派发；达到上限则入队延后，
  由外层派发循环兜底 drain，事件不丢弃，延后次数计入 ``bus.deferred``。
- 单个回调抛异常只隔离该回调，不影响同轮其他订阅者与后续事件，次数计入
  ``bus.errors``。
"""

from dataclasses import dataclass, field
from typing import Any, Callable, Optional


@dataclass
class Subscription:
    """一条订阅记录。"""

    topic: str
    priority: int
    seq: int
    handler: Callable[[Any], None]


@dataclass
class Bus:
    """事件总线。

    统计字段：
        dispatches:    已完成的事件派发次数（每个 publish 的事件恰好派发一次）
        deferred:      因触达深度上限而入队延后的事件数
        max_depth_seen:运行过程中观察到的最大嵌套深度
        errors:        回调异常被隔离的次数
        error_log:     异常明细 (topic, priority, seq, 异常类型名)
    """

    handlers: dict = field(default_factory=dict)
    queue: list = field(default_factory=list)
    depth: int = 0
    # 统计
    dispatches: int = 0
    deferred: int = 0
    max_depth_seen: int = 0
    errors: int = 0
    error_log: list = field(default_factory=list)
    # 内部状态
    _seq: int = 0
    _limit: int = 0  # 当前 dispatch 生效的深度上限，0 表示未在派发


def subscribe(bus: Bus, topic: str, handler: Callable[[Any], None],
              priority: int) -> Subscription:
    """订阅 topic。

    priority 为整数，数值越大越先被回调；同优先级按注册先后回调。
    返回创建的 :class:`Subscription`。
    """
    bus._seq += 1
    sub = Subscription(topic=topic, priority=int(priority),
                       seq=bus._seq, handler=handler)
    entries = bus.handlers.setdefault(topic, [])
    entries.append(sub)
    # 优先级降序、注册顺序升序；排序只作用于总线持有的列表，
    # 已经在派发中的快照不受影响。
    entries.sort(key=lambda s: (-s.priority, s.seq))
    return sub


def unsubscribe(bus: Bus, topic: str, priority: int,
                handler: Optional[Callable[[Any], None]] = None) -> int:
    """退订。

    匹配 ``topic`` 与 ``priority``（若再给出 ``handler`` 则同时要求是同一回调），
    返回移除的订阅条数。派发途中退订同样只影响后续轮次（快照语义）。
    """
    entries = bus.handlers.get(topic)
    if not entries:
        return 0
    if handler is None:
        kept = [s for s in entries if s.priority != priority]
    else:
        kept = [s for s in entries
                if not (s.priority == priority and s.handler == handler)]
    removed = len(entries) - len(kept)
    if kept:
        bus.handlers[topic] = kept
    else:
        bus.handlers.pop(topic, None)
    return removed


def publish(bus: Bus, topic: str, payload: Any) -> None:
    """发布一个事件。

    - 总线空闲（未在 ``dispatch`` 中）：入队，等待下一次 ``dispatch``。
    - 派发中且嵌套深度未达上限：就地嵌套派发（重入）。
    - 派发中且已达深度上限：入队延后并累加 ``deferred``，绝不丢弃，
      由外层派发循环在深度回落后处理。
    """
    event = (topic, payload)
    if bus._limit and bus.depth >= bus._limit:
        bus.queue.append(event)
        bus.deferred += 1
        return
    if bus._limit:
        _deliver(bus, event)
    else:
        bus.queue.append(event)


def dispatch(bus: Bus, max_depth: int) -> int:
    """派发队列中全部事件，直到队列排空，返回累计派发次数。

    ``max_depth`` 即可配置的嵌套深度上限（>=1）：顶层派发深度为 1，
    回调里嵌套 publish 最多再深入 ``max_depth - 1`` 层，超出的事件延后。
    """
    if not isinstance(max_depth, int) or max_depth < 1:
        raise ValueError("max_depth 必须是 >= 1 的整数")

    outer = bus._limit == 0
    if outer:
        bus._limit = max_depth
    try:
        while bus.queue:
            event = bus.queue.pop(0)
            _deliver(bus, event)
    finally:
        if outer:
            bus._limit = 0
    return bus.dispatches


def _deliver(bus: Bus, event: tuple) -> None:
    """把单个事件派发给其 topic 的当前订阅者快照。"""
    topic, payload = event
    bus.depth += 1
    bus.dispatches += 1
    if bus.depth > bus.max_depth_seen:
        bus.max_depth_seen = bus.depth
    try:
        # 快照：本轮派发固定使用派发开始时刻的订阅列表，
        # 途中 subscribe/unsubscribe 都改动不了这个局部副本。
        snapshot = list(bus.handlers.get(topic, ()))
        for sub in snapshot:
            try:
                sub.handler(payload)
            except Exception as exc:  # 异常隔离：只跳过当前回调
                bus.errors += 1
                bus.error_log.append(
                    (topic, sub.priority, sub.seq, type(exc).__name__))
    finally:
        bus.depth -= 1
