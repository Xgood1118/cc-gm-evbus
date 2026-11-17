# -*- coding: utf-8 -*-
"""命令行入口：读入事件序列并执行派发，落盘派发记录与统计。

内置一组确定性的演示订阅，用来展示嵌套、延后与异常隔离；
同一输入多次运行产出字节级一致的输出（不写时间戳、不依赖随机数）。
"""

import argparse
import json

import evbus as m


def build_demo(bus):
    """注册演示订阅，返回 trace 收集器。

    针对 data/events.json 中的两个 "t" 事件构造三层事件链
    ``t -> m -> n``，在默认深度上限 2 下：

    - ``t`` 三个订阅者演示优先级顺序与异常隔离；
    - ``t/p0`` 再 publish ``m``（深度 2），演示重入嵌套；
    - ``m`` 再 publish ``n`` 时已触达深度上限，``n`` 入队延后，
      由外层循环在深度 1 补派，事件不丢；
    - ``t/-1`` 每次抛异常，演示异常隔离不影响其他订阅者。
    """
    trace = []

    def t_high(payload):
        trace.append({"topic": "t", "handler": "t_p1",
                      "depth": bus.depth, "status": "ok"})

    def t_republish(payload):
        trace.append({"topic": "t", "handler": "t_p0",
                      "depth": bus.depth, "status": "ok"})
        m.publish(bus, "m", {})

    def t_boom(payload):
        trace.append({"topic": "t", "handler": "t_m1",
                      "depth": bus.depth, "status": "raising"})
        raise RuntimeError("boom from t_m1")

    def on_m(payload):
        trace.append({"topic": "m", "handler": "m_p0",
                      "depth": bus.depth, "status": "ok"})
        m.publish(bus, "n", {})  # 深度 2 触顶 -> n 延后

    def on_n(payload):
        trace.append({"topic": "n", "handler": "n_p0",
                      "depth": bus.depth, "status": "ok"})

    m.subscribe(bus, "t", t_boom, -1)
    m.subscribe(bus, "t", t_republish, 0)
    m.subscribe(bus, "t", t_high, 1)
    m.subscribe(bus, "m", on_m, 0)
    m.subscribe(bus, "n", on_n, 0)
    return trace


def main():
    ap = argparse.ArgumentParser(description="事件总线优先级与重入")
    ap.add_argument("input", help="事件序列 JSON 文件，如 data/events.json")
    ap.add_argument("--out", required=True, help="派发结果输出路径")
    ap.add_argument("--max-depth", type=int, default=2,
                    help="嵌套深度上限，默认 2")
    a = ap.parse_args()

    with open(a.input, encoding="utf-8") as f:
        events = json.load(f)

    bus = m.Bus()
    trace = build_demo(bus)
    for event in events:
        m.publish(bus, event["topic"], event.get("payload", {}))

    m.dispatch(bus, a.max_depth)

    result = {
        "input": a.input,
        "max_depth": a.max_depth,
        "trace": trace,
        "stats": {
            "dispatches": bus.dispatches,
            "deferred": bus.deferred,
            "max_depth_seen": bus.max_depth_seen,
            "errors": bus.errors,
        },
        "error_log": [
            {"topic": topic, "priority": priority, "seq": seq,
             "exception": exc_name}
            for topic, priority, seq, exc_name in bus.error_log
        ],
    }
    with open(a.out, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2, sort_keys=True)
        f.write("\n")

    print("dispatches={dispatches} deferred={deferred} "
          "max_depth_seen={max_depth_seen} errors={errors}".format(
              **result["stats"]))


if __name__ == "__main__":
    main()
