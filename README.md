# 事件总线优先级与重入

一个支持优先级、可重入派发且不丢事件的事件总线

## 保留签名

- `Bus`
- `subscribe(bus, topic, handler, priority)`
- `publish(bus, topic, payload)`
- `dispatch(bus, max_depth)`

## 自测

```
python -m pytest tests -q
python -m evbus.cli data/events.json --out dispatch.json
```
