# -*- coding: utf-8 -*-
"""事件总线优先级与重入"""

from dataclasses import dataclass, field


@dataclass
class Bus:
    """事件总线。"""
    handlers: dict = field(default_factory=dict)
    queue: list = field(default_factory=list)
    depth: int = 0


def subscribe(bus, topic, handler, priority):
    """订阅"""
    raise NotImplementedError("subscribe 尚未实现")


def publish(bus, topic, payload):
    """发布"""
    raise NotImplementedError("publish 尚未实现")


def dispatch(bus, max_depth):
    """派发"""
    raise NotImplementedError("dispatch 尚未实现")


def unsubscribe(path):
    """读入与落盘辅助"""
    raise NotImplementedError("unsubscribe 尚未实现")

