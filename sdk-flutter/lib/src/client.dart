import 'dart:async';
import 'dart:convert';
import 'package:http/http.dart' as http;
import 'package:meta/meta.dart';
import 'package:uuid/uuid.dart';

import 'event.dart';
import 'storage.dart';
import 'token_cache.dart';

/// Configuration for [DatacatClient].
@immutable
final class DatacatConfig {
  const DatacatConfig({
    required this.endpoint,
    required this.getToken,
    this.actorId,
    this.tenantId,
    this.sessionId,
    this.batchSize = 20,
    this.flushInterval = const Duration(seconds: 5),
    this.maxQueueSize = 1000,
    this.maxRetries = 5,
    this.httpClient,
    this.storage,
    this.onError,
  });

  /// Base URL of the Datacat ingestion API, e.g. `https://ingest.example.com/v1/events`.
  final String endpoint;

  /// Returns a short-lived JWT. Called by the SDK when needed; never stored in source.
  final Future<String> Function() getToken;

  /// Persistent actor identifier (1–200 chars).
  ///
  /// Optional: in B2B apps the user often authenticates *after* the SDK is
  /// initialised. Leave it `null` here and call [DatacatClient.identify] once
  /// the actor is known. Events tracked before an actor is set are dropped
  /// (see [DatacatClient.track]), since `actor_id` is required by the contract.
  final String? actorId;

  /// Optional tenant identifier for multi-tenant B2B apps.
  final String? tenantId;

  /// Optional session ID override. If omitted, the SDK generates and persists one.
  final String? sessionId;

  /// Number of events that trigger an automatic flush (default: 20).
  final int batchSize;

  /// Timer-based flush interval (default: 5 s).
  final Duration flushInterval;

  /// Maximum number of events kept in the in-memory queue (default: 1000).
  /// Oldest events are dropped when the queue is full.
  final int maxQueueSize;

  /// Maximum number of retry attempts per batch (default: 5).
  final int maxRetries;

  /// Injectable HTTP client (defaults to `http.Client()`). Pass a mock for tests.
  final http.Client? httpClient;

  /// Injectable storage (defaults to [InMemoryStorage]).
  /// For Flutter apps, provide a `shared_preferences`-backed implementation.
  final DatacatStorage? storage;

  /// Called when an event is permanently dropped (max retries exceeded or 400).
  final void Function(Object error, List<DatacatEvent> dropped)? onError;
}

/// Main analytics client.
///
/// Usage:
/// ```dart
/// final client = DatacatClient(config: DatacatConfig(
///   endpoint: 'https://ingest.example.com/v1/events',
///   getToken: () async => myBackend.getAnalyticsToken(),
///   actorId: currentUser.id,
/// ));
///
/// client.track('page_viewed', {'page': 'home'});
/// ```
///
/// Call [dispose] when the client is no longer needed. For Flutter apps,
/// call [flush] from a [WidgetsBindingObserver.didChangeAppLifecycleState]
/// callback on [AppLifecycleState.paused] / [AppLifecycleState.detached].
final class DatacatClient {
  DatacatClient({required DatacatConfig config}) : _config = config {
    _httpClient = config.httpClient ?? http.Client();
    _storage = config.storage ?? InMemoryStorage();
    _tokenCache = TokenCache(getToken: config.getToken);
    _currentActorId = config.actorId;
    _currentTenantId = config.tenantId;
    _scheduleFlush();
    _sessionReady = _initSession();
  }

  final DatacatConfig _config;
  late final http.Client _httpClient;
  late final DatacatStorage _storage;
  late final TokenCache _tokenCache;

  static const _sessionKey = 'datacat_session_id';
  static const _uuid = Uuid();

  /// Current actor/tenant identity. Initialised from config and mutable via
  /// [identify] (the user may authenticate after the SDK is created).
  String? _currentActorId;
  String? _currentTenantId;

  String? _sessionId;

  /// Completes once the session ID has been loaded/generated.
  late final Future<void> _sessionReady;

  final _queue = <DatacatEvent>[];
  final _retryCount = <String, int>{}; // eventId → retry count
  Timer? _flushTimer;
  bool _disposed = false;
  bool _flushing = false;

  // ── Public API ─────────────────────────────────────────────────────────────

  /// Sets (or updates) the actor identity used for subsequent events.
  ///
  /// In B2B apps the user often authenticates *after* the SDK is created; call
  /// this once the actor is known. Every event tracked afterwards carries the
  /// new [actorId] and [tenantId]. Passing `tenantId: null` clears the tenant.
  void identify({required String actorId, String? tenantId}) {
    _currentActorId = actorId;
    _currentTenantId = tenantId;
  }

  /// Queues an event.
  ///
  /// The call is synchronous from the caller's perspective: event_id,
  /// timestamp_client and the actor/tenant identity are frozen immediately. If
  /// the session ID is not yet available (async init still pending), the event
  /// is queued after session init completes automatically via [_sessionReady].
  ///
  /// If no actor identity is set (neither in [DatacatConfig.actorId] nor via
  /// [identify]), the event is **dropped** and [DatacatConfig.onError] is
  /// invoked: `actor_id` is required by the wire contract. The SDK never
  /// crashes nor sends an event without an actor.
  ///
  /// [properties] must NOT contain sensitive data (PII, credentials, health
  /// data). They are stored and transmitted in plain text.
  void track(String eventName, [Map<String, dynamic> properties = const {}]) {
    if (_disposed || _disposing) return;

    // Freeze the actor/tenant identity at track-time, alongside event_id and
    // timestamp_client.
    final actorId = _currentActorId;
    if (actorId == null || actorId.isEmpty) {
      _config.onError?.call(
        StateError(
          'track("$eventName") called without an actor_id. Call identify() '
          'before tracking, or set DatacatConfig.actorId. Event dropped.',
        ),
        const [],
      );
      return;
    }
    final tenantId = _currentTenantId;

    if (_sessionId == null) {
      // Session init is async. We capture the timestamp/id NOW (frozen) and
      // enqueue after the session is ready.
      final frozenId = _uuid.v4();
      final frozenTs = DateTime.now().toUtc().toIso8601String();
      unawaited(_sessionReady.then((_) {
        if (_disposed) return;
        final event = DatacatEvent(
          eventId: frozenId,
          eventName: eventName,
          actorId: actorId,
          sessionId: _sessionId!,
          timestampClient: frozenTs,
          tenantId: tenantId,
          properties: Map.unmodifiable(properties),
        );
        _enqueue(event);
      }));
      return;
    }

    final event = DatacatEvent(
      eventId: _uuid.v4(),
      eventName: eventName,
      actorId: actorId,
      sessionId: _sessionId!,
      timestampClient: DateTime.now().toUtc().toIso8601String(),
      tenantId: tenantId,
      properties: Map.unmodifiable(properties),
    );

    _enqueue(event);
  }

  /// Immediately flushes all queued events.
  ///
  /// Awaiting this method ensures delivery before, e.g., the app is backgrounded.
  Future<void> flush() async {
    if (_disposed) return;
    // Wait for session init so any deferred track() calls are enqueued first.
    await _sessionReady;
    await _flushInternal();
  }

  /// Cancels the flush timer and flushes remaining events.
  Future<void> dispose() async {
    if (_disposed) return;
    // Mark as disposed first so track() is rejected, but keep _disposed=false
    // long enough for the final flush to proceed. We use a separate flag.
    _disposing = true;
    _flushTimer?.cancel();
    // Ensure session init completes so deferred track() calls are enqueued.
    await _sessionReady;
    // Allow any pending microtasks (deferred track .then callbacks) to run.
    await Future.delayed(Duration.zero);
    _disposed = true;
    // Send any remaining events.
    await _flushInternal();
    _httpClient.close();
  }

  bool _disposing = false;

  Future<void> _flushInternal() async {
    if (_queue.isEmpty) return;
    await _sendBatch();
  }

  // ── Internal helpers ────────────────────────────────────────────────────────

  Future<void> _initSession() async {
    if (_config.sessionId != null) {
      _sessionId = _config.sessionId;
      return;
    }
    final stored = await _storage.read(_sessionKey);
    if (stored != null && stored.isNotEmpty) {
      _sessionId = stored;
    } else {
      _sessionId = _uuid.v4();
      await _storage.write(_sessionKey, _sessionId!);
    }
  }

  void _enqueue(DatacatEvent event) {
    if (_queue.length >= _config.maxQueueSize) {
      // Drop the oldest event to make room (loss tolerated, signalled via onError).
      final dropped = _queue.removeAt(0);
      _retryCount.remove(dropped.eventId);
      _config.onError?.call(
        StateError(
          'Queue full (maxQueueSize=${_config.maxQueueSize}): dropping oldest event',
        ),
        [dropped],
      );
    }
    _queue.add(event);
    if (_queue.length >= _config.batchSize) {
      unawaited(_sendBatch());
    }
  }

  void _scheduleFlush() {
    _flushTimer = Timer.periodic(_config.flushInterval, (_) {
      if (!_disposed && _queue.isNotEmpty) {
        unawaited(_sendBatch());
      }
    });
  }

  Future<void> _sendBatch() async {
    if (_flushing || _queue.isEmpty) return;
    _flushing = true;
    try {
      await _trySend();
    } finally {
      _flushing = false;
    }
  }

  Future<void> _trySend() async {
    // Take up to batchSize events, leaving the rest in the queue.
    final batch = _queue.take(_config.batchSize).toList();

    late http.Response response;
    try {
      final token = await _tokenCache.token();
      response = await _post(batch, token);
    } catch (e) {
      // Network error: keep events in queue, they will be retried later.
      _handleRetryableFailure(batch, e);
      return;
    }

    switch (response.statusCode) {
      case 200:
      case 201:
      case 202:
        // Success: remove the batch from the queue.
        _removeFromQueue(batch);
        for (final e in batch) {
          _retryCount.remove(e.eventId);
        }

      case 401:
        // Expired token: invalidate cache and retry immediately once.
        await _tokenCache.invalidate();
        try {
          final newToken = await _tokenCache.token();
          final retryResponse = await _post(batch, newToken);
          if (retryResponse.statusCode >= 200 &&
              retryResponse.statusCode < 300) {
            _removeFromQueue(batch);
            for (final e in batch) {
              _retryCount.remove(e.eventId);
            }
          } else {
            _handleRetryableFailure(
              batch,
              HttpException(retryResponse.statusCode, retryResponse.body),
            );
          }
        } catch (e) {
          _handleRetryableFailure(batch, e);
        }

      case 400:
      case 413:
        // Permanent failure: abandon the batch.
        _removeFromQueue(batch);
        for (final e in batch) {
          _retryCount.remove(e.eventId);
        }
        _config.onError?.call(
          HttpException(response.statusCode, response.body),
          batch,
        );

      case 429:
        // Rate limited: respect Retry-After if present.
        final retryAfter = _parseRetryAfter(response.headers['retry-after']);
        _handleRetryableFailure(
          batch,
          HttpException(response.statusCode, response.body),
          overrideDelay: retryAfter,
        );

      default:
        if (response.statusCode >= 500) {
          _handleRetryableFailure(
            batch,
            HttpException(response.statusCode, response.body),
          );
        } else {
          // Unexpected 4xx: abandon.
          _removeFromQueue(batch);
          _config.onError?.call(
            HttpException(response.statusCode, response.body),
            batch,
          );
        }
    }
  }

  Future<http.Response> _post(List<DatacatEvent> events, String token) {
    final body = jsonEncode({'events': events.map((e) => e.toJson()).toList()});
    return _httpClient.post(
      Uri.parse(_config.endpoint),
      headers: {
        'Content-Type': 'application/json',
        'Authorization': 'Bearer $token',
      },
      body: body,
    );
  }

  void _removeFromQueue(List<DatacatEvent> batch) {
    final ids = {for (final e in batch) e.eventId};
    _queue.removeWhere((e) => ids.contains(e.eventId));
  }

  void _handleRetryableFailure(
    List<DatacatEvent> batch,
    Object error, {
    Duration? overrideDelay,
  }) {
    final toRetry = <DatacatEvent>[];
    final toDrop = <DatacatEvent>[];

    for (final event in batch) {
      final attempts = (_retryCount[event.eventId] ?? 0) + 1;
      if (attempts > _config.maxRetries) {
        toDrop.add(event);
        _retryCount.remove(event.eventId);
      } else {
        _retryCount[event.eventId] = attempts;
        toRetry.add(event);
      }
    }

    if (toDrop.isNotEmpty) {
      _removeFromQueue(toDrop);
      _config.onError?.call(error, toDrop);
    }

    // Schedule a retry with exponential back-off. Events stay in _queue so the
    // next flush (timer or explicit) will pick them up — no additional scheduling
    // needed. However for 429 with a Retry-After header we schedule explicitly.
    if (overrideDelay != null && toRetry.isNotEmpty) {
      Future.delayed(overrideDelay, () {
        if (!_disposed) unawaited(_sendBatch());
      });
    }
  }

  static Duration? _parseRetryAfter(String? header) {
    if (header == null) return null;
    final seconds = int.tryParse(header.trim());
    if (seconds != null) return Duration(seconds: seconds);
    // RFC 7231 HTTP-date format: not implemented for simplicity; fall back.
    return null;
  }
}

/// Lightweight HTTP error carrier.
final class HttpException implements Exception {
  const HttpException(this.statusCode, this.body);
  final int statusCode;
  final String body;

  @override
  String toString() => 'HttpException($statusCode): $body';
}

// Silence the unawaited_futures lint for fire-and-forget calls.
void unawaited(Future<void> future) {}
