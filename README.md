# Kafka Scan GUI

一个基于 Python Tkinter 的 Kafka 图形化测试工具，用于批量检测 Kafka 目标、获取 topic 列表，可选获取 topic 消息详情，在内网攻防中快速证明kafka成果。

## 功能

- 支持未授权 Kafka 连接测试
- 支持 SASL/PLAIN 账号密码认证
- 支持 SOCKS5 代理
- 支持批量目标扫描，每行一个目标
- 支持获取 topic 列表
- 支持获取 topic 消息总数和最近消息内容
- 支持列表视图和图标视图
- 支持复制结果和导出 JSON

![列表模式](/img/5dddc1f0-6fc5-4161-b9a9-a143a78e2811.png)  
![卡片模式](/img/ccc4ae35-3482-4169-a3df-9c9295f256211.png)

## 环境要求

- Python 3.8+
- Tkinter
- 可访问目标 Kafka 的网络环境

macOS 如果系统 Python 的 Tkinter 不可用，建议使用带 Tk 8.6 的 Python，例如 Homebrew 或 python.org 安装的 Python。

## 安装依赖

```bash
pip3 install kafka-python PySocks
```

依赖说明：

- `kafka-python`：连接 Kafka、获取 topic、offset 和消息
- `PySocks`：启用 SOCKS5 代理时必需
- `tkinter`：Python GUI 库，通常随 Python 自带

如果运行时报 `No module named tkinter`，需要安装带 Tkinter 的 Python。

## 运行

```bash
git clone https://github.com/woods/kafka_scan_gui.git
cd kafka_scan_gui
python3 kafka_scan_gui.py
```

macOS 可按实际 Python 路径运行：

```bash
/usr/local/bin/python3 kafka_scan_gui.py
```

## 使用说明

### 1. 连接模式

工具支持两种模式：

- 未授权模式：适用于无需认证的 Kafka
- 认证模式：适用于 SASL/PLAIN 账号密码认证的 Kafka

认证模式需要填写：

- 用户名
- 密码

### 2. SOCKS5 代理

如需通过 SOCKS5 代理连接 Kafka，勾选“启用 SOCKS5 代理”，并填写：

- 地址，例如 `127.0.0.1`
- 端口，例如 `8090`
- 用户，可选
- 密码，可选

示例：

```text
地址: 127.0.0.1
端口: 8090
```

### 3. 扫描目标

目标 IP 输入框支持每行一个目标。

示例：

```text
127.0.0.1:9092
192.168.1.10:9092
0.0.0.0:9092
```

如果不填写端口，默认使用 `9092`。

```text
192.168.1.10
```

等价于：

```text
192.168.1.10:9092
```

### 4. 超时和重试

可配置：

- 超时(s)：单次连接或获取详情的等待时间
- 重试次数：失败后的重试次数

网络较慢或走代理时，可适当调大超时时间。

### 5. 获取消息详情

默认只获取 topic 列表。

如果需要查看每个 topic 的消息数量和最近消息，勾选“获取消息详情”，并设置“最近条数”。

例如设置为 `2`，表示每个 topic 最多展示最近 2 条消息。

## 结果说明

扫描成功后会展示：

- Kafka 目标地址
- topic 数量
- topic 名称
- topic 消息数量
- 最近消息内容

如果未勾选“获取消息详情”，只显示 topic 列表。

## 导出 JSON

点击“导出 JSON”可将当前扫描结果保存为 JSON 文件。

导出内容包含：

- 目标地址
- topic 列表
- topic 数量
- 消息详情
- 错误信息

## 常见问题

### PySocks 未安装

如果启用 SOCKS5 代理时报错：

```text
PySocks 未安装，请运行: pip3 install pysocks
```

执行：

```bash
pip3 install PySocks
```

### 能获取 topic，但获取消息详情超时

这种情况通常发生在 SOCKS5 代理场景。当前项目已处理 kafka-python 在 SOCKS5 连接后需要恢复非阻塞 socket 的问题。

如果仍然超时，可尝试：

- 调大超时时间
- 确认代理能访问 Kafka broker 返回的真实地址
- 确认 Kafka 的 broker 地址和端口可通过代理访问

### macOS 打不开 GUI

如果出现 Tkinter 相关错误，请使用带 Tk 8.6 的 Python 运行。

可检查：

```bash
python3 -m tkinter
```

如果无法打开 Tk 测试窗口，需要更换或安装支持 Tkinter 的 Python。

## 项目结构

```text
kafka_gui.py   # GUI 界面入口
kafka_core.py  # Kafka 连接、代理、扫描和消息获取逻辑
```

## 注意事项

- 扫描未授权 Kafka 或读取消息前，请确保你有合法授权。
- 获取消息详情会读取 topic 的 offset 和最近消息，可能触发 Kafka broker 的更多请求。
- 批量目标较多、topic 较多或代理较慢时，建议适当调大超时时间。
