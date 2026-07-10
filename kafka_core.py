import os
import socket
import time
import threading
import queue
from kafka import KafkaConsumer, TopicPartition

try:
    import socks
    HAS_SOCKS = True
except ImportError:
    HAS_SOCKS = False

_original_socket = socket.socket
_original_create_connection = socket.create_connection
_proxy_enabled = False
_proxy_cfg = None

class KafkaSocksSocket(socks.socksocket if HAS_SOCKS else _original_socket):
    connect_timeout = 10

    def setblocking(self, flag):
        _original_socket.setblocking(self, flag)
        self._timeout = None

    def settimeout(self, timeout):
        if timeout is not None:
            try:
                timeout = float(timeout)
                if timeout < 0:
                    timeout = 0.0
                elif timeout > 60:
                    timeout = 60.0
            except Exception:
                timeout = None
        _original_socket.settimeout(self, timeout)
        self._timeout = timeout

    def connect(self, address):
        return super().connect(address)

    def connect_ex(self, address):
        try:
            self.settimeout(self.connect_timeout)
            self.connect(address)
            self.setblocking(False)
            return 0
        except OSError as e:
            return getattr(e, 'errno', 1) or 1
        except Exception:
            return 1


def enable_socks5(host, port, username=None, password=None, timeout=10):
    global _proxy_enabled, _proxy_cfg
    if not HAS_SOCKS:
        raise ImportError("PySocks 未安装，请运行: pip3 install pysocks")
    KafkaSocksSocket.connect_timeout = timeout
    socks.set_default_proxy(socks.SOCKS5, host, int(port),
                            username=username or None,
                            password=password or None)
    socket.socket = KafkaSocksSocket
    socket.create_connection = _proxy_create_connection
    _proxy_cfg = (host, int(port), username or None, password or None)
    _proxy_enabled = True


def disable_socks5():
    global _proxy_enabled, _proxy_cfg
    socket.socket = _original_socket
    socket.create_connection = _original_create_connection
    _proxy_cfg = None
    _proxy_enabled = False


def _proxy_create_connection(address, timeout=None, source_address=None):
    sock = KafkaSocksSocket()
    if timeout is not None:
        sock.settimeout(timeout)
    if source_address:
        sock.bind(source_address)
    sock.connect(address)
    return sock


def test_socks5(host, port, username=None, password=None, timeout=8):
    if not HAS_SOCKS:
        return False, "PySocks 未安装"
    try:
        s = _original_socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(timeout)
        s.connect((host, int(port)))
        s.sendall(b'\x05\x01\x00')
        resp = s.recv(2)
        s.close()
        if resp == b'\x05\x00':
            return True, "SOCKS5 代理可用 (无需认证)"
        if resp == b'\x05\x02':
            if username and password:
                return True, "SOCKS5 代理可用 (需认证, 已提供)"
            return False, "SOCKS5 代理需要账号密码认证"
        return False, f"非预期响应: {resp.hex()}"
    except Exception as e:
        return False, f"代理连接失败: {e}"


class KafkaTester:
    def __init__(self, mode='unauth', username='', password='',
                 proxy_host='', proxy_port='', proxy_user='', proxy_pass='',
                 timeout=10, retries=5, fetch_messages=False,
                 message_count=2, log_func=None):
        self.mode = mode
        self.username = username
        self.password = password
        self.proxy_host = proxy_host
        self.proxy_port = proxy_port
        self.proxy_user = proxy_user
        self.proxy_pass = proxy_pass
        self.timeout = int(timeout)
        self.retries = int(retries)
        self.fetch_messages = bool(fetch_messages)
        self.message_count = int(message_count)
        self.log_func = log_func

    def ui_log(self, msg):
        if self.log_func:
            self.log_func(msg)

    def setup_proxy(self):
        if self.proxy_host and self.proxy_port:
            self.ui_log(f"检查 SOCKS5 代理 {self.proxy_host}:{self.proxy_port}...")
            ok, msg = test_socks5(self.proxy_host, self.proxy_port,
                                  self.proxy_user, self.proxy_pass,
                                  timeout=self.timeout)
            self.ui_log(msg)
            if not ok:
                raise RuntimeError(f"SOCKS5 代理不可用: {msg}")
            enable_socks5(self.proxy_host, self.proxy_port,
                          self.proxy_user, self.proxy_pass,
                          timeout=self.timeout)
        else:
            disable_socks5()

    def _safe_close(self, consumer, timeout=5):
        if not consumer:
            return
        try:
            self._run_with_timeout(lambda: consumer.close(), timeout, f"关闭连接超时({timeout}s)")
        except Exception:
            pass

    def _run_with_timeout(self, func, timeout, timeout_msg):
        q = queue.Queue(maxsize=1)

        def runner():
            try:
                q.put((True, func()))
            except Exception as e:
                q.put((False, e))

        t = threading.Thread(target=runner, daemon=True)
        t.start()
        try:
            ok, value = q.get(timeout=timeout)
        except queue.Empty:
            raise TimeoutError(timeout_msg)
        if ok:
            return value
        raise value

    def _build_conf(self, target):
        conf = {
            'bootstrap_servers': target if ':' in target else f'{target}:9092',
            'request_timeout_ms': (self.timeout + 5) * 1000,
            'session_timeout_ms': self.timeout * 1000,
            'api_version_auto_timeout_ms': self.timeout * 1000,
            'reconnect_backoff_ms': 1000,
            'reconnect_backoff_max_ms': 10000,
            'consumer_timeout_ms': 3000,
            'group_id': f'kafka-gui-{os.getpid()}',
            'auto_offset_reset': 'latest',
            'enable_auto_commit': False,
        }
        if self.mode == 'auth':
            conf['security_protocol'] = 'SASL_PLAINTEXT'
            conf['sasl_mechanism'] = 'PLAIN'
            conf['sasl_plain_username'] = self.username
            conf['sasl_plain_password'] = self.password
        return conf

    def check_and_fetch(self, target):
        conf = self._build_conf(target)
        last_error = None
        for i in range(self.retries):
            consumer = None
            try:
                self.ui_log(f"{target} 第 {i + 1}/{self.retries} 次连接 Kafka...")
                consumer = KafkaConsumer(**conf)
                self.ui_log(f"{target} Kafka 连接成功，开始获取 topic...")
                all_topics = self._run_with_timeout(
                    lambda: consumer.topics(),
                    self.timeout,
                    f"获取 topic 超时({self.timeout}s)"
                )
                topics = sorted(t for t in all_topics if not t.startswith('__'))
                details = {}
                detail_errors = 0
                self._safe_close(consumer)
                consumer = None
                if self.fetch_messages:
                    for topic in topics:
                        self.ui_log(f"{target} 获取 {topic} 消息详情...")
                        try:
                            details[topic] = self._get_topic_detail_with_retry(target, topic)
                        except Exception as e:
                            detail_errors += 1
                            details[topic] = {
                                'count': -1,
                                'messages': [],
                                'error': f"{type(e).__name__}: {e}",
                            }
                            self.ui_log(f"{target} 获取 {topic} 失败: {type(e).__name__}: {e}")
                return {
                    'success': True, 'ip': target, 'topics': topics,
                    'details': details, 'error': None,
                    'topic_count': len(topics),
                    'fetch_messages': self.fetch_messages,
                    'detail_errors': detail_errors,
                }
            except Exception as e:
                last_error = f"{type(e).__name__}: {e}"
                self.ui_log(f"{target} 第 {i + 1}/{self.retries} 次失败: {last_error}")
                if consumer:
                    self._safe_close(consumer)
                if i < self.retries - 1:
                    time.sleep(1)
        return {
            'success': False, 'ip': target, 'topics': [], 'details': {},
            'error': last_error, 'topic_count': 0,
            'fetch_messages': self.fetch_messages,
            'detail_errors': 0,
        }

    def _get_topic_detail_with_retry(self, target, topic):
        last_error = None
        for i in range(self.retries):
            consumer = None
            try:
                if i > 0:
                    self.ui_log(f"{target} 获取 {topic} 第 {i + 1}/{self.retries} 次重试...")
                consumer = KafkaConsumer(**self._build_conf(target))
                detail = self._run_with_timeout(
                    lambda: self._get_topic_detail(consumer, topic),
                    self.timeout,
                    f"获取 {topic} 消息详情超时({self.timeout}s)"
                )
                self._safe_close(consumer)
                return detail
            except Exception as e:
                last_error = e
                self._safe_close(consumer)
                if i < self.retries - 1:
                    time.sleep(1)
        raise last_error

    def _get_topic_detail(self, consumer, topic):
        partitions = consumer.partitions_for_topic(topic)
        if not partitions:
            return {'count': 0, 'messages': []}
        tps = [TopicPartition(topic, p) for p in partitions]
        beginning = consumer.beginning_offsets(tps)
        end = consumer.end_offsets(tps)
        total = sum(end[tp] - beginning[tp] for tp in tps)
        messages = []
        if total > 0:
            messages = self._fetch_messages(consumer, tps, beginning, end)
        return {'count': total, 'messages': messages}

    def _fetch_messages(self, consumer, tps, beginning, end):
        messages = []
        consumer.assign(tps)
        for tp in tps:
            start = max(end[tp] - self.message_count, beginning[tp])
            consumer.seek(tp, start)
        deadline = time.time() + min(max(self.timeout, 3), 30)
        while len(messages) < self.message_count and time.time() < deadline:
            records = consumer.poll(timeout_ms=1000, max_records=self.message_count)
            if not records:
                break
            for tp_list in records.values():
                for msg in tp_list:
                    try:
                        value = msg.value.decode('utf-8', errors='replace')
                    except Exception:
                        value = str(msg.value)
                    messages.append({
                        'partition': msg.partition,
                        'offset': msg.offset,
                        'value': value,
                    })
                    if len(messages) >= self.message_count:
                        break
                if len(messages) >= self.message_count:
                    break
        try:
            consumer.unsubscribe()
        except Exception:
            pass
        return messages
