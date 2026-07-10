#!/usr/bin/env python3
import tkinter as tk
from tkinter import ttk, messagebox, scrolledtext
import threading
import time
import os
import json
import concurrent.futures
from kafka_core import KafkaTester, HAS_SOCKS

MAX_MSG_DISPLAY = 120


class KafkaGUI:
    def __init__(self, root):
        self.root = root
        self.root.title("Kafka Scan GUI")
        self.root.geometry("1180x820")
        self.root.minsize(980, 680)
        self.results = {}
        self.scan_thread = None
        self.stop_event = threading.Event()
        self.scanning = False
        self.scan_id = 0
        self.icon_cards = []
        self._setup_style()
        self._build_ui()

    def _setup_style(self):
        self.colors = {
            'bg': '#f5f7fb',
            'card': '#ffffff',
            'line': '#e5e7eb',
            'text': '#111827',
            'muted': '#6b7280',
            'primary': '#2563eb',
            'ok': '#16a34a',
            'fail': '#dc2626',
            'warn': '#d97706',
        }
        self.root.configure(bg=self.colors['bg'])
        style = ttk.Style()
        try:
            style.theme_use('clam')
        except Exception:
            pass
        style.configure('.', font=('Helvetica', 12), background=self.colors['bg'], foreground=self.colors['text'])
        style.configure('App.TFrame', background=self.colors['bg'])
        style.configure('Card.TFrame', background=self.colors['card'], relief='flat')
        style.configure('CardTitle.TLabel', background=self.colors['card'], foreground=self.colors['text'], font=('Helvetica', 13, 'bold'))
        style.configure('Muted.TLabel', background=self.colors['card'], foreground=self.colors['muted'], font=('Helvetica', 11))
        style.configure('Title.TLabel', background=self.colors['bg'], foreground=self.colors['text'], font=('Helvetica', 22, 'bold'))
        style.configure('SubTitle.TLabel', background=self.colors['bg'], foreground=self.colors['muted'], font=('Helvetica', 12))
        style.configure('Primary.TButton', font=('Helvetica', 12, 'bold'), padding=(14, 8))
        style.configure('Tool.TButton', padding=(10, 6))
        style.configure('TEntry', padding=5)
        style.configure('Treeview', rowheight=30, font=('Helvetica', 11), background='#ffffff', fieldbackground='#ffffff')
        style.configure('Treeview.Heading', font=('Helvetica', 11, 'bold'))
        style.map('Treeview', background=[('selected', '#dbeafe')], foreground=[('selected', '#111827')])

    def _card(self, parent, title, subtitle=None):
        frame = ttk.Frame(parent, style='Card.TFrame', padding=14)
        ttk.Label(frame, text=title, style='CardTitle.TLabel').pack(anchor='w')
        if subtitle:
            ttk.Label(frame, text=subtitle, style='Muted.TLabel').pack(anchor='w', pady=(2, 10))
        else:
            ttk.Frame(frame, height=10, style='Card.TFrame').pack()
        return frame

    def _build_ui(self):
        main = ttk.Frame(self.root, style='App.TFrame', padding=16)
        main.pack(fill='both', expand=True)

        header = ttk.Frame(main, style='App.TFrame')
        header.pack(fill='x', pady=(0, 14))
        ttk.Label(header, text="Kafka Scan GUI", style='Title.TLabel').pack(side='left')
        self.status_badge = ttk.Label(header, text="就绪", style='SubTitle.TLabel')
        self.status_badge.pack(side='right', padx=(8, 0))
        ttk.Label(header, text="批量检测 Kafka、Topic 与最近消息", style='SubTitle.TLabel').pack(side='left', padx=16, pady=(8, 0))

        config = ttk.Frame(main, style='App.TFrame')
        config.pack(fill='x')
        for i in range(3):
            config.columnconfigure(i, weight=1, uniform='config')

        conn = self._card(config, "连接配置", "选择连接模式和认证信息")
        conn.grid(row=0, column=0, sticky='nsew', padx=(0, 8))
        self.mode_var = tk.StringVar(value='unauth')
        ttk.Radiobutton(conn, text="未授权模式", variable=self.mode_var, value='unauth', command=self._toggle_auth).pack(anchor='w', pady=2)
        ttk.Radiobutton(conn, text="SASL/PLAIN 认证", variable=self.mode_var, value='auth', command=self._toggle_auth).pack(anchor='w', pady=2)
        auth_grid = ttk.Frame(conn, style='Card.TFrame')
        auth_grid.pack(fill='x', pady=(10, 0))
        auth_grid.columnconfigure(1, weight=1)
        ttk.Label(auth_grid, text="用户名", style='Muted.TLabel').grid(row=0, column=0, sticky='w', pady=3)
        self.username_entry = ttk.Entry(auth_grid)
        self.username_entry.grid(row=0, column=1, sticky='ew', padx=(8, 0), pady=3)
        ttk.Label(auth_grid, text="密码", style='Muted.TLabel').grid(row=1, column=0, sticky='w', pady=3)
        self.password_entry = ttk.Entry(auth_grid, show='*')
        self.password_entry.grid(row=1, column=1, sticky='ew', padx=(8, 0), pady=3)

        proxy = self._card(config, "代理配置", "支持 SOCKS5 代理和认证")
        proxy.grid(row=0, column=1, sticky='nsew', padx=8)
        self.proxy_var = tk.BooleanVar(value=False)
        ttk.Checkbutton(proxy, text="启用 SOCKS5 代理", variable=self.proxy_var, command=self._toggle_proxy).pack(anchor='w', pady=(0, 8))
        proxy_grid = ttk.Frame(proxy, style='Card.TFrame')
        proxy_grid.pack(fill='x')
        for i in range(4):
            proxy_grid.columnconfigure(i, weight=1)
        ttk.Label(proxy_grid, text="地址", style='Muted.TLabel').grid(row=0, column=0, sticky='w')
        ttk.Label(proxy_grid, text="端口", style='Muted.TLabel').grid(row=0, column=1, sticky='w', padx=(8, 0))
        self.proxy_host_entry = ttk.Entry(proxy_grid)
        self.proxy_port_entry = ttk.Entry(proxy_grid)
        self.proxy_host_entry.grid(row=1, column=0, sticky='ew')
        self.proxy_port_entry.grid(row=1, column=1, sticky='ew', padx=(8, 0))
        ttk.Label(proxy_grid, text="用户", style='Muted.TLabel').grid(row=2, column=0, sticky='w', pady=(8, 0))
        ttk.Label(proxy_grid, text="密码", style='Muted.TLabel').grid(row=2, column=1, sticky='w', padx=(8, 0), pady=(8, 0))
        self.proxy_user_entry = ttk.Entry(proxy_grid)
        self.proxy_pass_entry = ttk.Entry(proxy_grid, show='*')
        self.proxy_user_entry.grid(row=3, column=0, sticky='ew')
        self.proxy_pass_entry.grid(row=3, column=1, sticky='ew', padx=(8, 0))

        scan = self._card(config, "扫描配置", "控制超时、重试和消息详情")
        scan.grid(row=0, column=2, sticky='nsew', padx=(8, 0))
        scan_grid = ttk.Frame(scan, style='Card.TFrame')
        scan_grid.pack(fill='x')
        for i in range(4):
            scan_grid.columnconfigure(i, weight=1)
        ttk.Label(scan_grid, text="超时(s)", style='Muted.TLabel').grid(row=0, column=0, sticky='w')
        ttk.Label(scan_grid, text="重试", style='Muted.TLabel').grid(row=0, column=1, sticky='w', padx=(8, 0))
        self.timeout_entry = ttk.Entry(scan_grid)
        self.timeout_entry.insert(0, "10")
        self.retries_entry = ttk.Entry(scan_grid)
        self.retries_entry.insert(0, "5")
        self.timeout_entry.grid(row=1, column=0, sticky='ew')
        self.retries_entry.grid(row=1, column=1, sticky='ew', padx=(8, 0))
        self.fetch_detail_var = tk.BooleanVar(value=False)
        ttk.Checkbutton(scan, text="获取消息详情", variable=self.fetch_detail_var, command=self._toggle_fetch_detail).pack(anchor='w', pady=(12, 4))
        msg_row = ttk.Frame(scan, style='Card.TFrame')
        msg_row.pack(fill='x')
        ttk.Label(msg_row, text="最近条数", style='Muted.TLabel').pack(side='left')
        self.message_count_entry = ttk.Entry(msg_row, width=8)
        self.message_count_entry.insert(0, "2")
        self.message_count_entry.pack(side='left', padx=(8, 0))

        target_card = self._card(main, "目标列表", "每行一个目标，可带端口；不写端口默认 9092")
        target_card.pack(fill='x', pady=14)
        target_body = ttk.Frame(target_card, style='Card.TFrame')
        target_body.pack(fill='x')
        self.ip_text = tk.Text(target_body, height=4, relief='flat', highlightthickness=1, highlightbackground=self.colors['line'], font=('Menlo', 12), wrap='none')
        self.ip_text.pack(side='left', fill='x', expand=True)
        ip_scroll = ttk.Scrollbar(target_body, command=self.ip_text.yview)
        ip_scroll.pack(side='right', fill='y')
        self.ip_text.configure(yscrollcommand=ip_scroll.set)
        ttk.Label(target_card, text="示例：127.0.0.1:9092、0.0.0.0:9092、192.168.1.10", style='Muted.TLabel').pack(anchor='w', pady=(8, 0))

        ctrl = ttk.Frame(main, style='App.TFrame')
        ctrl.pack(fill='x', pady=(0, 10))
        self.scan_btn = ttk.Button(ctrl, text="开始扫描", style='Primary.TButton', command=self.start_scan)
        self.scan_btn.pack(side='left')
        self.stop_btn = ttk.Button(ctrl, text="停止", style='Tool.TButton', command=self.stop_scan, state='disabled')
        self.stop_btn.pack(side='left', padx=8)
        ttk.Button(ctrl, text="清空结果", style='Tool.TButton', command=self.clear_results).pack(side='left')
        ttk.Button(ctrl, text="清空日志", style='Tool.TButton', command=self.clear_log).pack(side='left', padx=8)
        ttk.Button(ctrl, text="导出 JSON", style='Tool.TButton', command=self.export_results).pack(side='left')
        view_frame = ttk.Frame(ctrl, style='App.TFrame')
        view_frame.pack(side='right')
        self.view_var = tk.StringVar(value='list')
        ttk.Radiobutton(view_frame, text="列表", variable=self.view_var, value='list', command=self._switch_view).pack(side='left', padx=4)
        ttk.Radiobutton(view_frame, text="卡片", variable=self.view_var, value='icon', command=self._switch_view).pack(side='left', padx=4)

        prog = ttk.Frame(main, style='App.TFrame')
        prog.pack(fill='x', pady=(0, 10))
        self.progress = ttk.Progressbar(prog, mode='determinate')
        self.progress.pack(side='left', fill='x', expand=True)
        self.status_label = ttk.Label(prog, text="0 / 0", style='SubTitle.TLabel', width=24)
        self.status_label.pack(side='right', padx=(10, 0))

        body = ttk.PanedWindow(main, orient='vertical')
        body.pack(fill='both', expand=True)
        self.result_container = ttk.Frame(body, style='App.TFrame')
        body.add(self.result_container, weight=4)
        self.log_outer = self._card(body, "运行日志")
        body.add(self.log_outer, weight=1)

        self.list_frame = ttk.Frame(self.result_container, style='App.TFrame')
        self._build_list_view(self.list_frame)
        self.icon_frame = ttk.Frame(self.result_container, style='App.TFrame')
        self._build_icon_view(self.icon_frame)
        self._switch_view()

        self.log_text = scrolledtext.ScrolledText(self.log_outer, height=6, font=('Menlo', 11), relief='flat', highlightthickness=1, highlightbackground=self.colors['line'])
        self.log_text.pack(fill='both', expand=True)
        self.log_text.bind('<Key>', self._log_readonly)
        self.log_text.bind('<Control-c>', self._copy_text_widget)
        self.log_text.bind('<Button-3>', self._show_log_menu)
        self._toggle_auth()
        self._toggle_proxy()
        self._toggle_fetch_detail()

    def _build_list_view(self, parent):
        cols = ('status', 'detail')
        self.tree = ttk.Treeview(parent, columns=cols, show='tree headings', selectmode='browse')
        self.tree.heading('#0', text='目标 / Topic / 消息')
        self.tree.heading('status', text='状态')
        self.tree.heading('detail', text='详情')
        self.tree.column('#0', width=620, stretch=True)
        self.tree.column('status', width=120, anchor='center')
        self.tree.column('detail', width=420, stretch=True)
        self.tree.tag_configure('ok', foreground=self.colors['ok'])
        self.tree.tag_configure('fail', foreground=self.colors['fail'])
        self.tree.tag_configure('warn', foreground=self.colors['warn'])
        tree_scroll = ttk.Scrollbar(parent, command=self.tree.yview)
        tree_xscroll = ttk.Scrollbar(parent, orient='horizontal', command=self.tree.xview)
        self.tree.configure(yscrollcommand=tree_scroll.set, xscrollcommand=tree_xscroll.set)
        parent.rowconfigure(0, weight=1)
        parent.columnconfigure(0, weight=1)
        self.tree.grid(row=0, column=0, sticky='nsew')
        tree_scroll.grid(row=0, column=1, sticky='ns')
        tree_xscroll.grid(row=1, column=0, sticky='ew')
        self.tree.bind('<Control-c>', lambda e: self._copy_treeview_item())
        self.tree.bind('<Button-3>', self._show_tree_menu)

    def _build_icon_view(self, parent):
        self.icon_canvas = tk.Canvas(parent, highlightthickness=0, background=self.colors['bg'])
        icon_scroll = ttk.Scrollbar(parent, command=self.icon_canvas.yview)
        self.icon_inner = ttk.Frame(self.icon_canvas, style='App.TFrame')
        self.icon_window = self.icon_canvas.create_window((0, 0), window=self.icon_inner, anchor='nw')
        self.icon_inner.bind('<Configure>', lambda e: self.icon_canvas.configure(scrollregion=self.icon_canvas.bbox('all')))
        self.icon_canvas.configure(yscrollcommand=icon_scroll.set)
        self.icon_canvas.pack(fill='both', expand=True, side='left')
        icon_scroll.pack(fill='y', side='right')
        self.icon_canvas.bind('<Configure>', self._on_icon_canvas_resize)

    def _show_log_menu(self, event):
        menu = tk.Menu(self.log_text, tearoff=0)
        menu.add_command(label="复制选中", command=lambda: self._copy_text_widget_for(self.log_text))
        menu.add_command(label="全选并复制", command=self._copy_all_log)
        menu.tk_popup(event.x_root, event.y_root)

    def _show_tree_menu(self, event):
        item = self.tree.identify_row(event.y)
        if item:
            self.tree.selection_set(item)
            menu = tk.Menu(self.tree, tearoff=0)
            menu.add_command(label="复制本行", command=self._copy_treeview_item)
            menu.add_command(label="复制全部结果", command=self._copy_treeview_all)
            menu.tk_popup(event.x_root, event.y_root)

    def _on_icon_canvas_resize(self, event):
        self.icon_canvas.itemconfigure(self.icon_window, width=event.width)
        self._layout_icon_cards(event.width)

    def _layout_icon_cards(self, width):
        card_w = 260
        gap = 12
        per_row = max(1, (width + gap) // (card_w + gap))
        for i, card in enumerate(self.icon_cards):
            r, c = divmod(i, per_row)
            card.grid_forget()
            card.grid(row=r, column=c, padx=gap // 2, pady=gap // 2, sticky='nsew')

    def _toggle_auth(self):
        state = 'normal' if self.mode_var.get() == 'auth' and not self.scanning else 'disabled'
        self.username_entry.configure(state=state)
        self.password_entry.configure(state=state)

    def _toggle_proxy(self):
        state = 'normal' if self.proxy_var.get() and not self.scanning else 'disabled'
        for w in [self.proxy_host_entry, self.proxy_port_entry, self.proxy_user_entry, self.proxy_pass_entry]:
            w.configure(state=state)

    def _toggle_fetch_detail(self):
        state = 'normal' if self.fetch_detail_var.get() and not self.scanning else 'disabled'
        self.message_count_entry.configure(state=state)

    def _set_inputs_state(self, enabled):
        state = 'normal' if enabled else 'disabled'
        for w in [self.timeout_entry, self.retries_entry, self.ip_text]:
            w.configure(state=state)
        self._toggle_auth()
        self._toggle_proxy()
        self._toggle_fetch_detail()

    def _switch_view(self):
        if self.view_var.get() == 'list':
            self.icon_frame.pack_forget()
            self.list_frame.pack(fill='both', expand=True)
        else:
            self.list_frame.pack_forget()
            self.icon_frame.pack(fill='both', expand=True)
            self.root.after(50, lambda: self._layout_icon_cards(self.icon_canvas.winfo_width()))

    def log(self, msg):
        self.log_text.insert('end', f"[{time.strftime('%H:%M:%S')}] {msg}\n")
        self.log_text.see('end')

    def clear_log(self):
        self.log_text.configure(state='normal')
        self.log_text.delete('1.0', 'end')

    def _log_readonly(self, event):
        if event.keysym in ('c', 'C') and (event.state & 0x4):
            return None
        if event.keysym in ('Left', 'Right', 'Up', 'Down', 'Home', 'End'):
            return None
        return 'break'

    def _copy_text_widget(self, event=None):
        try:
            w = event.widget if event else self.root.focus_get()
            if hasattr(w, 'selection_get'):
                text = w.selection_get()
            else:
                return
        except Exception:
            return
        self.root.clipboard_clear()
        self.root.clipboard_append(text)

    def _copy_text_widget_for(self, widget):
        try:
            text = widget.selection_get()
        except Exception:
            return
        self.root.clipboard_clear()
        self.root.clipboard_append(text)

    def _copy_all_log(self):
        content = self.log_text.get('1.0', 'end')
        self.root.clipboard_clear()
        self.root.clipboard_append(content)

    def _copy_treeview_item(self):
        sel = self.tree.selection()
        if not sel:
            return
        item = sel[0]
        text = self.tree.item(item, 'text')
        values = self.tree.item(item, 'values')
        self.root.clipboard_clear()
        self.root.clipboard_append(' | '.join([text] + [str(v) for v in values if v]))

    def _copy_treeview_all(self):
        lines = []
        for item in self.tree.get_children(''):
            self._collect_tree(item, lines, 0)
        self.root.clipboard_clear()
        self.root.clipboard_append('\n'.join(lines))

    def _collect_tree(self, item, lines, depth):
        text = self.tree.item(item, 'text')
        values = self.tree.item(item, 'values')
        extra = ' | '.join(str(v) for v in values if v)
        lines.append('  ' * depth + (f"{text} | {extra}" if extra else text))
        for child in self.tree.get_children(item):
            self._collect_tree(child, lines, depth + 1)

    def _parse_ips(self):
        raw = self.ip_text.get('1.0', 'end').strip()
        return [line.strip() for line in raw.splitlines() if line.strip() and not line.strip().startswith('#')]

    def start_scan(self):
        if self.scanning:
            return
        ips = self._parse_ips()
        if not ips:
            messagebox.showwarning("提示", "请输入至少一个目标 IP")
            return
        if self.mode_var.get() == 'auth' and not self.username_entry.get().strip():
            messagebox.showwarning("提示", "认证模式需要填写用户名")
            return
        try:
            timeout = int(self.timeout_entry.get().strip())
            retries = int(self.retries_entry.get().strip())
            message_count = int(self.message_count_entry.get().strip() or "0")
            if timeout <= 0 or retries <= 0:
                raise ValueError
            if self.fetch_detail_var.get() and message_count <= 0:
                raise ValueError
        except ValueError:
            messagebox.showwarning("提示", "超时、重试次数、消息条数必须是正整数")
            return
        if self.proxy_var.get():
            if not HAS_SOCKS:
                messagebox.showerror("错误", "PySocks 未安装，请运行: pip3 install PySocks")
                return
            if not self.proxy_host_entry.get().strip() or not self.proxy_port_entry.get().strip():
                messagebox.showwarning("提示", "请填写代理地址和端口")
                return

        self.clear_results()
        self.scan_id += 1
        scan_id = self.scan_id
        self.stop_event.clear()
        self.scanning = True
        self.scan_btn.configure(state='disabled')
        self.stop_btn.configure(state='normal')
        self.progress['value'] = 0
        self.progress['maximum'] = len(ips)
        self.status_label.configure(text=f"0 / {len(ips)}")
        self.status_badge.configure(text="扫描中")
        self._set_inputs_state(False)

        tester = KafkaTester(
            mode=self.mode_var.get(),
            username=self.username_entry.get().strip(),
            password=self.password_entry.get().strip(),
            proxy_host=self.proxy_host_entry.get().strip() if self.proxy_var.get() else '',
            proxy_port=self.proxy_port_entry.get().strip() if self.proxy_var.get() else '',
            proxy_user=self.proxy_user_entry.get().strip() if self.proxy_var.get() else '',
            proxy_pass=self.proxy_pass_entry.get().strip() if self.proxy_var.get() else '',
            timeout=timeout,
            retries=retries,
            fetch_messages=self.fetch_detail_var.get(),
            message_count=message_count,
            log_func=lambda msg, sid=scan_id: self.root.after(0, self.log, msg) if sid == self.scan_id else None,
        )
        self.log(f"开始扫描 {len(ips)} 个目标，模式={tester.mode}，代理={'是' if tester.proxy_host else '否'}")
        self.scan_thread = threading.Thread(target=self._scan_worker, args=(ips, tester, scan_id), daemon=True)
        self.scan_thread.start()

    def stop_scan(self):
        if not self.scanning:
            return
        self.stop_event.set()
        self.scan_id += 1
        self.scanning = False
        self.scan_btn.configure(state='normal')
        self.stop_btn.configure(state='disabled')
        self.status_badge.configure(text="已停止")
        self.status_label.configure(text="已停止，可重新开始")
        self._set_inputs_state(True)
        self.log("已请求停止扫描，后台连接会在超时后自动释放")

    def _scan_worker(self, ips, tester, scan_id):
        try:
            tester.setup_proxy()
        except Exception as e:
            if scan_id == self.scan_id:
                self.root.after(0, self._on_scan_error, str(e))
            return

        completed = 0
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            futures = {}
            for ip in ips:
                if self.stop_event.is_set():
                    break
                self.root.after(0, self.log, f"开始连接 {ip}...")
                futures[executor.submit(tester.check_and_fetch, ip)] = ip

            for fut in concurrent.futures.as_completed(futures):
                if self.stop_event.is_set():
                    break
                ip = futures[fut]
                try:
                    result = fut.result()
                except Exception as e:
                    result = {'success': False, 'ip': ip, 'topics': [], 'details': {}, 'error': str(e), 'topic_count': 0, 'fetch_messages': tester.fetch_messages}
                completed += 1
                if scan_id == self.scan_id:
                    self.root.after(0, self._on_ip_done, result, completed, len(ips))

        if scan_id == self.scan_id:
            self.root.after(0, self._on_scan_complete)

    def _on_ip_done(self, result, completed, total):
        self.results[result['ip']] = result
        self.progress['value'] = completed
        ip = result['ip']
        if result['success']:
            detail_errors = result.get('detail_errors', 0)
            if detail_errors:
                self.log(f"成功 {ip}，{result['topic_count']} 个 topic，{detail_errors} 个详情失败")
            else:
                self.log(f"成功 {ip}，{result['topic_count']} 个 topic")
        else:
            self.log(f"失败 {ip}: {result['error']}")
        self.status_label.configure(text=f"{completed} / {total}")
        self._add_to_list_view(result)
        self._add_to_icon_view(result)

    def _on_scan_complete(self):
        self.scanning = False
        self.scan_btn.configure(state='normal')
        self.stop_btn.configure(state='disabled')
        self._set_inputs_state(True)
        ok = sum(1 for r in self.results.values() if r['success'])
        fail = len(self.results) - ok
        topics = sum(r.get('topic_count', 0) for r in self.results.values() if r.get('success'))
        self.status_badge.configure(text="完成")
        self.status_label.configure(text=f"成功 {ok}，失败 {fail}，Topic {topics}")
        self.log(f"扫描完成：成功 {ok}，失败 {fail}，Topic 总数 {topics}")

    def _on_scan_error(self, err):
        self.scanning = False
        self.scan_btn.configure(state='normal')
        self.stop_btn.configure(state='disabled')
        self._set_inputs_state(True)
        self.status_badge.configure(text="错误")
        self.log(f"错误：{err}")
        messagebox.showerror("错误", err)

    def clear_results(self):
        self.results.clear()
        if hasattr(self, 'tree'):
            for item in self.tree.get_children():
                self.tree.delete(item)
        for card in self.icon_cards:
            card.destroy()
        self.icon_cards.clear()
        if hasattr(self, 'progress'):
            self.progress['value'] = 0
        if hasattr(self, 'status_label'):
            self.status_label.configure(text="0 / 0")
        if hasattr(self, 'status_badge'):
            self.status_badge.configure(text="就绪")

    def _add_to_list_view(self, result):
        ip = result['ip']
        if result['success']:
            tag = 'ok' if not result.get('detail_errors') else 'warn'
            status = '成功' if tag == 'ok' else '部分失败'
            label = f"{ip}"
            detail = f"Topic: {result['topic_count']}"
        else:
            tag = 'fail'
            status = '失败'
            label = f"{ip}"
            detail = result.get('error', '') or ''
        ip_node = self.tree.insert('', 'end', text=label, values=(status, detail), open=True, tags=(tag,))
        if not result['success']:
            return
        for topic in result['topics']:
            if not result.get('fetch_messages'):
                self.tree.insert(ip_node, 'end', text=topic, values=('Topic', '未获取消息详情'))
                continue
            detail_data = result['details'].get(topic, {})
            count = detail_data.get('count', 0)
            err = detail_data.get('error')
            if err:
                topic_node = self.tree.insert(ip_node, 'end', text=topic, values=('错误', err[:120]), tags=('warn',))
            else:
                topic_node = self.tree.insert(ip_node, 'end', text=topic, values=('Topic', f"消息数: {count}"))
            for msg in detail_data.get('messages', []):
                val = msg['value']
                if len(val) > MAX_MSG_DISPLAY:
                    val = val[:MAX_MSG_DISPLAY] + '...'
                self.tree.insert(topic_node, 'end', text=val, values=(f"P{msg['partition']}", f"Offset {msg['offset']}"))

    def _add_to_icon_view(self, result):
        card = ttk.Frame(self.icon_inner, style='Card.TFrame', padding=14)
        ip = result['ip']
        if result['success']:
            status = '成功'
            color = self.colors['ok'] if not result.get('detail_errors') else self.colors['warn']
            info = f"Topic {result.get('topic_count', 0)}"
        else:
            status = '失败'
            color = self.colors['fail']
            info = result.get('error', '未知错误')[:50]
        head = ttk.Frame(card, style='Card.TFrame')
        head.pack(fill='x')
        ttk.Label(head, text=ip, style='CardTitle.TLabel').pack(side='left')
        ttk.Label(head, text=status, foreground=color, background=self.colors['card'], font=('Helvetica', 12, 'bold')).pack(side='right')
        ttk.Label(card, text=info, style='Muted.TLabel').pack(anchor='w', pady=(8, 10))
        ttk.Button(card, text="查看详情", command=lambda i=ip: self._show_ip_detail(i)).pack(fill='x')
        self.icon_cards.append(card)
        self._layout_icon_cards(self.icon_canvas.winfo_width())

    def _show_ip_detail(self, ip):
        result = self.results.get(ip)
        if not result:
            return
        win = tk.Toplevel(self.root)
        win.title(f"{ip} - 详情")
        win.geometry("860x620")
        win.configure(bg=self.colors['bg'])
        top = ttk.Frame(win, style='App.TFrame', padding=12)
        top.pack(fill='x')
        if result['success']:
            ttk.Label(top, text=f"{ip}", style='Title.TLabel').pack(side='left')
            ttk.Label(top, text=f"Topic {result['topic_count']}", style='SubTitle.TLabel').pack(side='right', pady=(8, 0))
        else:
            ttk.Label(top, text=f"{ip} 连接失败", style='Title.TLabel').pack(anchor='w')
            ttk.Label(top, text=result.get('error', ''), style='SubTitle.TLabel').pack(anchor='w')
            return
        if not result.get('fetch_messages'):
            txt = scrolledtext.ScrolledText(win, font=('Menlo', 12), wrap='word')
            txt.pack(fill='both', expand=True, padx=12, pady=12)
            txt.insert('end', "本次未获取消息详情，仅获取 Topic 列表。\n\n")
            for topic in result['topics']:
                txt.insert('end', f"{topic}\n")
            txt.configure(state='disabled')
            return
        nb = ttk.Notebook(win)
        nb.pack(fill='both', expand=True, padx=12, pady=(0, 12))
        for topic in result['topics']:
            detail = result['details'].get(topic, {})
            tab = ttk.Frame(nb, padding=8)
            nb.add(tab, text=topic[:20])
            count = detail.get('count', 0)
            err = detail.get('error')
            if err:
                ttk.Label(tab, text=f"错误：{err}", foreground=self.colors['fail']).pack(anchor='w')
            else:
                ttk.Label(tab, text=f"消息总数：{count}").pack(anchor='w')
            txt = scrolledtext.ScrolledText(tab, font=('Menlo', 11), wrap='word')
            txt.pack(fill='both', expand=True, pady=(8, 0))
            for msg in detail.get('messages', []):
                txt.insert('end', f"[Partition {msg['partition']} | Offset {msg['offset']}]\n{msg['value']}\n{'-' * 80}\n")
            if not detail.get('messages'):
                txt.insert('end', "无可展示消息\n")
            txt.configure(state='disabled')

    def export_results(self):
        if not self.results:
            messagebox.showinfo("提示", "暂无结果可导出")
            return
        path = os.path.join(os.getcwd(), f'kafka_results_{int(time.time())}.json')
        export = {}
        for ip, r in self.results.items():
            export[ip] = {
                'success': r['success'],
                'topic_count': r.get('topic_count', 0),
                'error': r.get('error'),
                'fetch_messages': r.get('fetch_messages', False),
                'topic_names': r.get('topics', []),
                'topics': {t: r['details'].get(t, {}) for t in r.get('topics', [])},
            }
        with open(path, 'w', encoding='utf-8') as f:
            json.dump(export, f, ensure_ascii=False, indent=2)
        self.log(f"已导出：{path}")
        messagebox.showinfo("导出成功", f"已保存到:\n{path}")


def main():
    root = tk.Tk()
    root.withdraw()
    KafkaGUI(root)
    root.update_idletasks()
    root.deiconify()
    root.lift()
    root.attributes('-topmost', True)
    root.after(100, lambda: root.attributes('-topmost', False))
    root.mainloop()


if __name__ == "__main__":
    main()
