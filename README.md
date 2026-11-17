# 事件总线优先级与重入

一个支持优先级、可重入派发且不丢事件的事件总线。仅使用 Python 标准库。

## 保留签名

- `Bus`
- `subscribe(bus, topic, handler, priority)`
- `publish(bus, topic, payload)`
- `dispatch(bus, max_depth)`
- `unsubscribe(bus, topic, priority, handler=None)` — 按 topic + 优先级退订，
  可选指定具体回调；返回移除条数

## 派发规则

1. **优先级顺序**：同一 topic 的订阅者按 `priority` 数值**降序**执行
   （数值越大越早）；同优先级按**注册顺序**执行。
2. **本轮快照**：每个事件派发开始时对订阅列表做一次快照。派发过程中
   `subscribe` / `unsubscribe` 只改动总线持有的列表，不影响本轮快照，
   新增订阅在**下一轮**派发才生效。
3. **重入与深度**：`max_depth` 是可配置的嵌套深度上限（顶层派发深度为 1）。
   回调中再次 `publish` 时：
   - 未达上限：就地嵌套派发，深度 +1；
   - 已达上限：事件进入队列**延后**处理，绝不丢弃，并计入 `bus.deferred`。
   外层 `dispatch` 循环会在深度回落后排空队列，因此每个 publish 的事件
   恰好被派发一次。
4. **确定性**：无时间戳、无随机数，同一事件序列两次派发的回调顺序完全一致。

## 边界行为（均有确定结果）

| 情况 | 结果 |
| --- | --- |
| 无订阅者 | 事件仍计为一次派发（`dispatches` +1），队列正常排空，不报错 |
| 回调中退订自己 | 本轮回调执行完毕；快照保证后续轮次不再调用该订阅者 |
| 回调抛异常 | 仅隔离该回调：同轮其余订阅者与后续事件不受影响，`errors` +1，明细进 `error_log` |

## 统计字段（`Bus` 上）

- `dispatches`：已完成的事件派发次数
- `deferred`：因触达深度上限而入队延后的事件数
- `max_depth_seen`：运行中观察到的最大嵌套深度
- `errors` / `error_log`：异常隔离次数及明细

## 自测

```
python -m pytest tests -q
python -m evbus.cli data/events.json --out dispatch.json
```

## CLI 实测数据

命令：`python -m evbus.cli data/events.json --out dispatch.json`
（输入为 `data/events.json` 中两个 `t` 事件，内置演示订阅构造
`t -> m -> n` 三层事件链，默认深度上限 `--max-depth 2`）

| 指标 | 值 |
| --- | --- |
| 派发次数 `dispatches` | 6 |
| 延后事件数 `deferred` | 2 |
| 最大嵌套深度 `max_depth_seen` | 2 |
| 异常隔离次数 `errors` | 2 |

派发顺序（详见 `dispatch.json` 的 `trace`）：每个 `t` 事件按优先级
`p1 → p0 → -1` 回调；`p0` 在深度 2 嵌套派发 `m`，`m` 再发布的 `n`
触顶延后，待外层回到深度 1 后补派；`-1` 回调抛出的 `RuntimeError`
被隔离两次，不影响其他回调。两次运行输出字节级一致。
