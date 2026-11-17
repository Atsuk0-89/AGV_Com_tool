import 'dart:async';
import 'dart:convert';
import 'dart:io';

import 'package:flutter/material.dart';
import 'package:path_provider/path_provider.dart';

void main() {
  WidgetsFlutterBinding.ensureInitialized();
  runApp(const SocketTesterApp());
}

enum Transport { tcp, udp }
enum Role { client, server }
enum DataFormat { hex, ascii }

String transportName(Transport value) => value == Transport.tcp ? 'TCP' : 'UDP';
String roleName(Role value) => value == Role.client ? 'Client' : 'Server';

class SessionConfig {
  SessionConfig({
    required this.name,
    required this.transport,
    required this.role,
    required this.remoteHost,
    required this.remotePort,
    required this.localAddress,
    required this.localPort,
  });

  final String name;
  final Transport transport;
  final Role role;
  final String remoteHost;
  final int remotePort;
  final String localAddress;
  final int localPort;
}

class TrafficLog {
  TrafficLog({
    required this.time,
    required this.direction,
    required this.bytes,
    required this.detail,
    this.remoteAddress = '',
    this.remotePort = 0,
  });

  final DateTime time;
  final String direction;
  final List<int> bytes;
  final String detail;
  final String remoteAddress;
  final int remotePort;

  String get hex => bytes.map((b) => b.toRadixString(16).padLeft(2, '0').toUpperCase()).join(' ');
  String get ascii => bytes.map((b) => (b >= 32 && b <= 126) ? String.fromCharCode(b) : '.').join();

  String toCsv() {
    String q(String s) => '"${s.replaceAll('"', '""')}"';
    return [
      q(time.toIso8601String()),
      q(direction),
      q(remoteAddress),
      remotePort.toString(),
      bytes.length.toString(),
      q(hex),
      q(ascii),
      q(detail),
    ].join(',');
  }
}

class SocketSession {
  SocketSession({
    required this.config,
    required this.onChanged,
    required this.onLog,
  });

  final SessionConfig config;
  final VoidCallback onChanged;
  final void Function(TrafficLog log) onLog;

  Socket? _tcpClient;
  ServerSocket? _tcpServer;
  final Map<int, Socket> _tcpPeers = {};
  int _nextPeerId = 1;
  RawDatagramSocket? _udp;
  InternetAddress? _lastUdpAddress;
  int? _lastUdpPort;
  Timer? _periodicTimer;
  bool _running = false;
  String status = 'Stopped';
  String lastError = '';
  int txPackets = 0;
  int rxPackets = 0;
  int txBytes = 0;
  int rxBytes = 0;
  final List<TrafficLog> logs = [];

  bool get isRunning => _running;
  int get peerCount => _tcpPeers.length;

  void _addLog(TrafficLog log) {
    logs.insert(0, log);
    if (logs.length > 5000) logs.removeLast();
    onLog(log);
    onChanged();
  }

  void _recordTx(List<int> data, String address, int port) {
    txPackets++;
    txBytes += data.length;
    _addLog(TrafficLog(
      time: DateTime.now(),
      direction: 'TX',
      bytes: List<int>.from(data),
      remoteAddress: address,
      remotePort: port,
      detail: 'Sent ${data.length} bytes',
    ));
  }

  void _recordRx(List<int> data, String address, int port) {
    rxPackets++;
    rxBytes += data.length;
    _addLog(TrafficLog(
      time: DateTime.now(),
      direction: 'RX',
      bytes: List<int>.from(data),
      remoteAddress: address,
      remotePort: port,
      detail: 'Received ${data.length} bytes',
    ));
  }

  Future<void> start() async {
    if (_running) return;
    lastError = '';
    status = 'Starting...';
    onChanged();
    try {
      if (config.transport == Transport.tcp) {
        if (config.role == Role.client) {
          await _startTcpClient();
        } else {
          await _startTcpServer();
        }
      } else {
        await _startUdp();
      }
      _running = true;
      if (config.transport == Transport.tcp && config.role == Role.server) {
        status = 'Listening';
      } else if (config.transport == Transport.udp && config.role == Role.server) {
        status = 'Listening';
      } else {
        status = 'Connected';
      }
    } catch (e) {
      lastError = e.toString();
      status = 'Error';
      await _disposeSockets();
    }
    onChanged();
  }

  InternetAddress _localInternetAddress() {
    if (config.localAddress.trim().isEmpty || config.localAddress == '0.0.0.0') {
      return InternetAddress.anyIPv4;
    }
    return InternetAddress(config.localAddress.trim());
  }

  Future<void> _startTcpClient() async {
    final remote = InternetAddress.lookup(config.remoteHost);
    final addresses = await remote;
    if (addresses.isEmpty) throw SocketException('Host not found: ${config.remoteHost}');
    _tcpClient = await Socket.connect(
      addresses.first,
      config.remotePort,
      sourceAddress: _localInternetAddress(),
      sourcePort: config.localPort,
      timeout: const Duration(seconds: 8),
    );
    _tcpClient!.setOption(SocketOption.tcpNoDelay, true);
    _tcpClient!.listen(
      (data) => _recordRx(data, _tcpClient!.remoteAddress.address, _tcpClient!.remotePort ?? 0),
      onError: (Object error) {
        lastError = error.toString();
        status = 'Error';
        onChanged();
      },
      onDone: () {
        _tcpClient = null;
        _running = false;
        status = 'Disconnected';
        onChanged();
      },
      cancelOnError: true,
    );
  }

  Future<void> _startTcpServer() async {
    _tcpServer = await ServerSocket.bind(
      _localInternetAddress(),
      config.localPort,
      shared: false,
    );
    _tcpServer!.listen((socket) {
      final id = _nextPeerId++;
      _tcpPeers[id] = socket;
      status = 'Listening (${_tcpPeers.length} client(s))';
      onChanged();
      socket.listen(
        (data) => _recordRx(data, socket.remoteAddress.address, socket.remotePort),
        onError: (Object error) {
          _addLog(TrafficLog(
            time: DateTime.now(),
            direction: 'SYS',
            bytes: const [],
            detail: 'Client $id error: $error',
          ));
        },
        onDone: () {
          _tcpPeers.remove(id);
          status = _running ? 'Listening (${_tcpPeers.length} client(s))' : 'Stopped';
          _addLog(TrafficLog(
            time: DateTime.now(),
            direction: 'SYS',
            bytes: const [],
            detail: 'Client ${socket.remoteAddress.address}:${socket.remotePort} disconnected',
          ));
          onChanged();
        },
        cancelOnError: true,
      );
      _addLog(TrafficLog(
        time: DateTime.now(),
        direction: 'SYS',
        bytes: const [],
        remoteAddress: socket.remoteAddress.address,
        remotePort: socket.remotePort,
        detail: 'Client connected',
      ));
    });
  }

  Future<void> _startUdp() async {
    _udp = await RawDatagramSocket.bind(
      _localInternetAddress(),
      config.localPort,
      reuseAddress: false,
    );
    _udp!.readEventsEnabled = true;
    _udp!.listen((event) {
      if (event == RawSocketEvent.read) {
        Datagram? datagram;
        while ((datagram = _udp?.receive()) != null) {
          final d = datagram!;
          _lastUdpAddress = d.address;
          _lastUdpPort = d.port;
          _recordRx(d.data, d.address.address, d.port);
        }
      } else if (event == RawSocketEvent.closed) {
        _running = false;
        status = 'Closed';
        onChanged();
      }
    }, onError: (Object error) {
      lastError = error.toString();
      status = 'Error';
      onChanged();
    });
  }

  Future<void> send(List<int> data) async {
    if (!_running) throw StateError('Session is not running');
    if (data.isEmpty) throw ArgumentError('No data to send');

    if (config.transport == Transport.tcp) {
      if (config.role == Role.client) {
        final socket = _tcpClient;
        if (socket == null) throw StateError('TCP client is not connected');
        socket.add(data);
        await socket.flush();
        _recordTx(data, socket.remoteAddress.address, socket.remotePort);
      } else {
        if (_tcpPeers.isEmpty) throw StateError('No TCP client connected');
        // Server sends to the most recently accepted client.
        final socket = _tcpPeers.values.last;
        socket.add(data);
        await socket.flush();
        _recordTx(data, socket.remoteAddress.address, socket.remotePort);
      }
    } else {
      final udp = _udp;
      if (udp == null) throw StateError('UDP socket is not available');
      InternetAddress address;
      int port;
      if (config.role == Role.client) {
        final addresses = await InternetAddress.lookup(config.remoteHost);
        if (addresses.isEmpty) throw SocketException('Host not found: ${config.remoteHost}');
        address = addresses.first;
        port = config.remotePort;
      } else {
        if (_lastUdpAddress == null || _lastUdpPort == null) {
          throw StateError('No UDP sender received yet');
        }
        address = _lastUdpAddress!;
        port = _lastUdpPort!;
      }
      final sent = udp.send(data, address, port);
      if (sent != data.length) throw SocketException('UDP send incomplete: $sent/${data.length}');
      _recordTx(data, address.address, port);
    }
  }

  void startPeriodic(List<int> data, Duration interval, int count) {
    stopPeriodic();
    if (interval.inMicroseconds <= 0) throw ArgumentError('Interval must be positive');
    var sent = 0;
    _periodicTimer = Timer.periodic(interval, (timer) async {
      if (!_running) {
        timer.cancel();
        return;
      }
      try {
        await send(data);
        sent++;
        if (count > 0 && sent >= count) timer.cancel();
      } catch (e) {
        lastError = e.toString();
        _addLog(TrafficLog(
          time: DateTime.now(),
          direction: 'ERR',
          bytes: const [],
          detail: 'Periodic send error: $e',
        ));
        timer.cancel();
      }
      onChanged();
    });
  }

  void stopPeriodic() {
    _periodicTimer?.cancel();
    _periodicTimer = null;
    onChanged();
  }

  Future<void> stop() async {
    stopPeriodic();
    _running = false;
    await _disposeSockets();
    status = 'Stopped';
    onChanged();
  }

  Future<void> _disposeSockets() async {
    try { await _tcpClient?.close(); } catch (_) {}
    _tcpClient = null;
    for (final socket in _tcpPeers.values) {
      try { await socket.close(); } catch (_) {}
    }
    _tcpPeers.clear();
    try { await _tcpServer?.close(); } catch (_) {}
    _tcpServer = null;
    _udp?.close();
    _udp = null;
  }

  void clearLogs() {
    logs.clear();
    txPackets = 0;
    rxPackets = 0;
    txBytes = 0;
    rxBytes = 0;
    onChanged();
  }
}

class SocketTesterApp extends StatelessWidget {
  const SocketTesterApp({super.key});

  @override
  Widget build(BuildContext context) {
    return MaterialApp(
      title: 'Socket Tester',
      debugShowCheckedModeBanner: false,
      theme: ThemeData(
        colorScheme: ColorScheme.fromSeed(seedColor: const Color(0xFF1769AA)),
        useMaterial3: true,
        inputDecorationTheme: const InputDecorationTheme(
          border: OutlineInputBorder(),
          isDense: true,
        ),
      ),
      home: const HomeScreen(),
    );
  }
}

class HomeScreen extends StatefulWidget {
  const HomeScreen({super.key});

  @override
  State<HomeScreen> createState() => _HomeScreenState();
}

class _HomeScreenState extends State<HomeScreen> {
  final List<SocketSession> _sessions = [];
  SocketSession? _selected;
  final _dataController = TextEditingController(text: '48 65 6C 6C 6F');
  final _intervalController = TextEditingController(text: '100');
  final _countController = TextEditingController(text: '0');
  DataFormat _format = DataFormat.hex;
  bool _periodic = false;
  String _message = '';

  @override
  void dispose() {
    for (final session in _sessions) {
      session.stop();
    }
    _dataController.dispose();
    _intervalController.dispose();
    _countController.dispose();
    super.dispose();
  }

  void _refresh() {
    if (mounted) setState(() {});
  }

  Future<void> _addSession() async {
    final config = await showDialog<SessionConfig>(
      context: context,
      builder: (_) => const AddSessionDialog(),
    );
    if (config == null) return;

    final session = SocketSession(
      config: config,
      onChanged: _refresh,
      onLog: (_) {},
    );
    setState(() {
      _sessions.add(session);
      _selected = session;
    });
    await session.start();
    _refresh();
  }

  Future<void> _removeSession(SocketSession session) async {
    await session.stop();
    setState(() {
      _sessions.remove(session);
      if (_selected == session) {
        _selected = _sessions.isEmpty ? null : _sessions.first;
      }
    });
  }

  List<int> _parseData() {
    if (_format == DataFormat.ascii) return utf8.encode(_dataController.text);
    final cleaned = _dataController.text
        .replaceAll(',', ' ')
        .replaceAll('0x', '')
        .replaceAll('0X', '')
        .trim();
    if (cleaned.isEmpty) return [];
    final compact = cleaned.replaceAll(RegExp(r'\s+'), '');
    if (compact.length.isOdd) throw FormatException('HEX must contain an even number of digits');
    if (!RegExp(r'^[0-9a-fA-F]+$').hasMatch(compact)) {
      throw FormatException('Invalid HEX characters');
    }
    return List<int>.generate(
      compact.length ~/ 2,
      (i) => int.parse(compact.substring(i * 2, i * 2 + 2), radix: 16),
    );
  }

  Future<void> _send() async {
    final session = _selected;
    if (session == null) return;
    try {
      await session.send(_parseData());
      setState(() => _message = '送信しました');
    } catch (e) {
      setState(() => _message = '送信エラー: $e');
    }
  }

  void _togglePeriodic() {
    final session = _selected;
    if (session == null) return;
    if (_periodic) {
      session.stopPeriodic();
      setState(() => _periodic = false);
      return;
    }
    try {
      final intervalMs = int.parse(_intervalController.text);
      final count = int.parse(_countController.text);
      if (intervalMs < 1 || count < 0) throw const FormatException('Invalid value');
      session.startPeriodic(_parseData(), Duration(milliseconds: intervalMs), count);
      setState(() {
        _periodic = true;
        _message = '周期送信を開始しました';
      });
    } catch (e) {
      setState(() => _message = '設定エラー: $e');
    }
  }

  Future<void> _saveCsv() async {
    final session = _selected;
    if (session == null) return;
    try {
      final directory = await getApplicationDocumentsDirectory();
      final safeName = session.config.name.replaceAll(RegExp(r'[^a-zA-Z0-9_-]'), '_');
      final file = File('${directory.path}/socket_${safeName}_${DateTime.now().millisecondsSinceEpoch}.csv');
      final header = 'Timestamp,Direction,RemoteIP,RemotePort,Length,HEX,ASCII,Detail';
      final rows = session.logs.reversed.map((e) => e.toCsv()).toList();
      await file.writeAsString('$header\n${rows.join('\n')}\n', encoding: utf8);
      setState(() => _message = 'CSVを保存しました: ${file.path}');
    } catch (e) {
      setState(() => _message = '保存エラー: $e');
    }
  }

  @override
  Widget build(BuildContext context) {
    final selected = _selected;
    return Scaffold(
      appBar: AppBar(
        title: const Text('Socket Tester'),
        actions: [
          IconButton(
            tooltip: 'セッション追加',
            onPressed: _addSession,
            icon: const Icon(Icons.add_circle_outline),
          ),
          const SizedBox(width: 8),
        ],
      ),
      body: LayoutBuilder(builder: (context, constraints) {
        final wide = constraints.maxWidth >= 760;
        final sessionPanel = _buildSessionPanel();
        final detail = selected == null ? _emptyDetail() : _buildDetail(selected);
        if (wide) {
          return Row(
            children: [
              SizedBox(width: 290, child: sessionPanel),
              const VerticalDivider(width: 1),
              Expanded(child: detail),
            ],
          );
        }
        return Column(
          children: [
            SizedBox(height: 190, child: sessionPanel),
            const Divider(height: 1),
            Expanded(child: detail),
          ],
        );
      }),
    );
  }

  Widget _buildSessionPanel() {
    return Column(
      children: [
        Padding(
          padding: const EdgeInsets.fromLTRB(16, 12, 16, 6),
          child: Row(
            children: [
              const Expanded(child: Text('通信セッション', style: TextStyle(fontWeight: FontWeight.bold))),
              Text('${_sessions.length}'),
              IconButton(onPressed: _addSession, icon: const Icon(Icons.add)),
            ],
          ),
        ),
        Expanded(
          child: _sessions.isEmpty
              ? const Center(child: Text('＋ から通信セッションを追加'))
              : ListView.builder(
                  itemCount: _sessions.length,
                  itemBuilder: (context, index) {
                    final session = _sessions[index];
                    final selected = session == _selected;
                    return ListTile(
                      selected: selected,
                      leading: Icon(
                        session.config.transport == Transport.tcp ? Icons.cable : Icons.wifi_tethering,
                      ),
                      title: Text(session.config.name, maxLines: 1, overflow: TextOverflow.ellipsis),
                      subtitle: Text('${transportName(session.config.transport)} ${roleName(session.config.role)} · ${session.status}'),
                      trailing: IconButton(
                        tooltip: '削除',
                        icon: const Icon(Icons.close, size: 18),
                        onPressed: () => _removeSession(session),
                      ),
                      onTap: () => setState(() {
                        _selected = session;
                        _periodic = false;
                      }),
                    );
                  },
                ),
        ),
      ],
    );
  }

  Widget _emptyDetail() => const Center(
    child: Column(
      mainAxisSize: MainAxisSize.min,
      children: [
        Icon(Icons.settings_ethernet, size: 56, color: Colors.grey),
        SizedBox(height: 12),
        Text('通信セッションを追加してください'),
      ],
    ),
  );

  Widget _buildDetail(SocketSession session) {
    final c = session.config;
    return Column(
      children: [
        Padding(
          padding: const EdgeInsets.fromLTRB(16, 12, 16, 8),
          child: Row(
            children: [
              Expanded(
                child: Column(
                  crossAxisAlignment: CrossAxisAlignment.start,
                  children: [
                    Text(c.name, style: Theme.of(context).textTheme.titleLarge),
                    Text(
                      '${transportName(c.transport)} ${roleName(c.role)} · '
                      '${c.role == Role.client ? '${c.remoteHost}:${c.remotePort}' : '${c.localAddress}:${c.localPort}'}',
                      style: Theme.of(context).textTheme.bodySmall,
                    ),
                  ],
                ),
              ),
              _statusChip(session.status),
              const SizedBox(width: 8),
              IconButton(
                tooltip: session.isRunning ? '切断' : '開始',
                onPressed: () async {
                  if (session.isRunning) {
                    await session.stop();
                  } else {
                    await session.start();
                  }
                  _refresh();
                },
                icon: Icon(session.isRunning ? Icons.stop_circle_outlined : Icons.play_circle_outline),
              ),
            ],
          ),
        ),
        if (session.lastError.isNotEmpty)
          Container(
            width: double.infinity,
            margin: const EdgeInsets.symmetric(horizontal: 16),
            padding: const EdgeInsets.all(8),
            decoration: BoxDecoration(color: Colors.red.withValues(alpha: .08), borderRadius: BorderRadius.circular(8)),
            child: Text(session.lastError, style: const TextStyle(color: Colors.red)),
          ),
        Padding(
          padding: const EdgeInsets.symmetric(horizontal: 16, vertical: 6),
          child: Row(
            children: [
              _stat('TX', session.txPackets, session.txBytes, Colors.green),
              const SizedBox(width: 8),
              _stat('RX', session.rxPackets, session.rxBytes, Colors.blue),
              if (c.role == Role.server && c.transport == Transport.tcp) ...[
                const SizedBox(width: 8),
                _stat('Peers', session.peerCount, 0, Colors.orange),
              ],
            ],
          ),
        ),
        const Divider(height: 1),
        Expanded(
          child: session.logs.isEmpty
              ? const Center(child: Text('通信ログはまだありません'))
              : ListView.builder(
                  reverse: true,
                  padding: const EdgeInsets.symmetric(horizontal: 12, vertical: 8),
                  itemCount: session.logs.length,
                  itemBuilder: (context, index) {
                    final log = session.logs[index];
                    final color = log.direction == 'TX'
                        ? Colors.green
                        : log.direction == 'RX'
                            ? Colors.blue
                            : Colors.orange;
                    return Container(
                      margin: const EdgeInsets.only(bottom: 6),
                      padding: const EdgeInsets.all(9),
                      decoration: BoxDecoration(
                        color: Theme.of(context).colorScheme.surfaceContainerHighest.withValues(alpha: .45),
                        borderRadius: BorderRadius.circular(8),
                      ),
                      child: Column(
                        crossAxisAlignment: CrossAxisAlignment.start,
                        children: [
                          Row(
                            children: [
                              Text(log.direction, style: TextStyle(color: color, fontWeight: FontWeight.bold)),
                              const SizedBox(width: 8),
                              Expanded(
                                child: Text(
                                  _formatTime(log.time),
                                  style: Theme.of(context).textTheme.labelSmall,
                                ),
                              ),
                              Text('${log.bytes.length} byte(s)', style: Theme.of(context).textTheme.labelSmall),
                            ],
                          ),
                          if (log.bytes.isNotEmpty) ...[
                            const SizedBox(height: 5),
                            SelectableText(log.hex, style: const TextStyle(fontFamily: 'monospace', fontSize: 13)),
                            Text('ASCII: ${log.ascii}', style: Theme.of(context).textTheme.bodySmall),
                            if (log.remoteAddress.isNotEmpty)
                              Text('${log.remoteAddress}:${log.remotePort}', style: Theme.of(context).textTheme.labelSmall),
                          ] else
                            Text(log.detail, style: Theme.of(context).textTheme.bodySmall),
                        ],
                      ),
                    );
                  },
                ),
        ),
        _buildSendPanel(session),
      ],
    );
  }

  Widget _statusChip(String status) {
    final active = status == 'Connected' || status.startsWith('Listening');
    return Chip(
      visualDensity: VisualDensity.compact,
      label: Text(status, style: const TextStyle(fontSize: 11)),
      backgroundColor: (active ? Colors.green : status == 'Error' ? Colors.red : Colors.grey)
          .withValues(alpha: .12),
    );
  }

  Widget _stat(String label, int packets, int bytes, Color color) {
    return Expanded(
      child: Container(
        padding: const EdgeInsets.all(9),
        decoration: BoxDecoration(
          color: color.withValues(alpha: .08),
          borderRadius: BorderRadius.circular(8),
        ),
        child: Column(
          crossAxisAlignment: CrossAxisAlignment.start,
          children: [
            Text(label, style: TextStyle(color: color, fontWeight: FontWeight.bold)),
            Text('$packets packets', style: const TextStyle(fontWeight: FontWeight.w600)),
            Text('$bytes bytes', style: Theme.of(context).textTheme.labelSmall),
          ],
        ),
      ),
    );
  }

  Widget _buildSendPanel(SocketSession session) {
    return Material(
      elevation: 4,
      child: Padding(
        padding: const EdgeInsets.all(12),
        child: Column(
          mainAxisSize: MainAxisSize.min,
          children: [
            Row(
              children: [
                SegmentedButton<DataFormat>(
                  segments: const [
                    ButtonSegment(value: DataFormat.hex, label: Text('HEX')),
                    ButtonSegment(value: DataFormat.ascii, label: Text('ASCII')),
                  ],
                  selected: {_format},
                  onSelectionChanged: (s) => setState(() => _format = s.first),
                ),
                const Spacer(),
                TextButton.icon(
                  onPressed: session.clearLogs,
                  icon: const Icon(Icons.delete_outline),
                  label: const Text('ログ消去'),
                ),
                IconButton(
                  tooltip: 'CSV保存',
                  onPressed: _saveCsv,
                  icon: const Icon(Icons.save_alt),
                ),
              ],
            ),
            const SizedBox(height: 8),
            TextField(
              controller: _dataController,
              minLines: 1,
              maxLines: 3,
              decoration: InputDecoration(
                labelText: _format == DataFormat.hex ? '送信データ（例: 01 03 00 FF）' : '送信データ',
                suffixIcon: IconButton(
                  tooltip: '送信',
                  onPressed: session.isRunning ? _send : null,
                  icon: const Icon(Icons.send),
                ),
              ),
              onSubmitted: (_) => _send(),
            ),
            const SizedBox(height: 8),
            Row(
              children: [
                SizedBox(
                  width: 105,
                  child: TextField(
                    controller: _intervalController,
                    keyboardType: TextInputType.number,
                    decoration: const InputDecoration(labelText: '周期 ms'),
                  ),
                ),
                const SizedBox(width: 8),
                SizedBox(
                  width: 105,
                  child: TextField(
                    controller: _countController,
                    keyboardType: TextInputType.number,
                    decoration: const InputDecoration(labelText: '回数 (0=無限)'),
                  ),
                ),
                const SizedBox(width: 8),
                Expanded(
                  child: FilledButton.tonal(
                    onPressed: session.isRunning ? _togglePeriodic : null,
                    child: Text(_periodic ? '周期送信停止' : '周期送信開始'),
                  ),
                ),
              ],
            ),
            if (_message.isNotEmpty)
              Align(
                alignment: Alignment.centerLeft,
                child: Padding(
                  padding: const EdgeInsets.only(top: 6),
                  child: Text(_message, style: Theme.of(context).textTheme.bodySmall),
                ),
              ),
          ],
        ),
      ),
    );
  }

  String _formatTime(DateTime time) {
    String two(int n) => n.toString().padLeft(2, '0');
    String six(int n) => n.toString().padLeft(6, '0');
    return '${two(time.hour)}:${two(time.minute)}:${two(time.second)}.${six(time.microsecond)}';
  }
}

class AddSessionDialog extends StatefulWidget {
  const AddSessionDialog({super.key});

  @override
  State<AddSessionDialog> createState() => _AddSessionDialogState();
}

class _AddSessionDialogState extends State<AddSessionDialog> {
  final _name = TextEditingController(text: 'New Session');
  final _host = TextEditingController(text: '192.168.0.100');
  final _remotePort = TextEditingController(text: '10001');
  final _localAddress = TextEditingController(text: '0.0.0.0');
  final _localPort = TextEditingController(text: '0');
  Transport _transport = Transport.tcp;
  Role _role = Role.client;
  String _error = '';

  @override
  void dispose() {
    _name.dispose();
    _host.dispose();
    _remotePort.dispose();
    _localAddress.dispose();
    _localPort.dispose();
    super.dispose();
  }

  int _port(TextEditingController controller, {bool allowZero = false}) {
    final value = int.parse(controller.text);
    if (value < (allowZero ? 0 : 1) || value > 65535) {
      throw FormatException('ポート番号は${allowZero ? '0' : '1'}～65535で指定してください');
    }
    return value;
  }

  void _create() {
    try {
      final localPort = _port(_localPort, allowZero: true);
      final remotePort = _port(_remotePort);
      if (_name.text.trim().isEmpty) throw const FormatException('セッション名を入力してください');
      if (_role == Role.client && _host.text.trim().isEmpty) {
        throw const FormatException('接続先ホストを入力してください');
      }
      if (_role == Role.server && localPort == 0) {
        throw const FormatException('Serverの待受ポートは1～65535で指定してください');
      }
      Navigator.pop(
        context,
        SessionConfig(
          name: _name.text.trim(),
          transport: _transport,
          role: _role,
          remoteHost: _host.text.trim(),
          remotePort: remotePort,
          localAddress: _localAddress.text.trim().isEmpty ? '0.0.0.0' : _localAddress.text.trim(),
          localPort: localPort,
        ),
      );
    } catch (e) {
      setState(() => _error = e.toString());
    }
  }

  @override
  Widget build(BuildContext context) {
    final server = _role == Role.server;
    return AlertDialog(
      title: const Text('通信セッション追加'),
      content: SizedBox(
        width: 440,
        child: SingleChildScrollView(
          child: Column(
            mainAxisSize: MainAxisSize.min,
            children: [
              TextField(controller: _name, decoration: const InputDecoration(labelText: 'セッション名')),
              const SizedBox(height: 12),
              DropdownButtonFormField<Transport>(
                value: _transport,
                decoration: const InputDecoration(labelText: 'プロトコル'),
                items: Transport.values.map((v) => DropdownMenuItem(value: v, child: Text(transportName(v)))).toList(),
                onChanged: (v) => setState(() => _transport = v!),
              ),
              const SizedBox(height: 12),
              DropdownButtonFormField<Role>(
                value: _role,
                decoration: const InputDecoration(labelText: '動作モード'),
                items: Role.values.map((v) => DropdownMenuItem(value: v, child: Text(roleName(v)))).toList(),
                onChanged: (v) => setState(() => _role = v!),
              ),
              const SizedBox(height: 12),
              if (!server) ...[
                TextField(controller: _host, decoration: const InputDecoration(labelText: '接続先IP / ホスト名')),
                const SizedBox(height: 12),
                TextField(
                  controller: _remotePort,
                  keyboardType: TextInputType.number,
                  decoration: const InputDecoration(labelText: '接続先ポート'),
                ),
                const SizedBox(height: 12),
              ],
              TextField(
                controller: _localAddress,
                decoration: const InputDecoration(labelText: 'ローカルIP（0.0.0.0 = 全IF）'),
              ),
              const SizedBox(height: 12),
              TextField(
                controller: _localPort,
                keyboardType: TextInputType.number,
                decoration: InputDecoration(
                  labelText: server ? '待受ポート' : 'ローカルポート（0 = 自動割当）',
                ),
              ),
              if (_transport == Transport.udp && server)
                const Padding(
                  padding: EdgeInsets.only(top: 8),
                  child: Text('UDP Serverは受信した相手のIP・ポートを返信先として使用します。',
                    style: TextStyle(fontSize: 12, color: Colors.grey)),
                ),
              if (_error.isNotEmpty)
                Padding(
                  padding: const EdgeInsets.only(top: 10),
                  child: Text(_error, style: const TextStyle(color: Colors.red)),
                ),
            ],
          ),
        ),
      ),
      actions: [
        TextButton(onPressed: () => Navigator.pop(context), child: const Text('キャンセル')),
        FilledButton(onPressed: _create, child: const Text('作成・開始')),
      ],
    );
  }
}
