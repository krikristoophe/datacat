import 'dart:async';
import 'dart:convert';

import 'package:datacat_sdk/datacat_sdk.dart';
import 'package:http/http.dart' as http;
import 'package:http/testing.dart';
import 'package:test/test.dart';

// ── Helpers ──────────────────────────────────────────────────────────────────

/// Sentinel so tests can explicitly pass `actorId: null` and override the default.
const _noActor = Object();

/// Builds a client with sensible test defaults.
///
/// Pass `actorId: null` to start the client without an actor identity.
DatacatClient _makeClient({
  MockClient? httpClient,
  int batchSize = 100, // large so we control flushes manually
  Duration flushInterval = const Duration(hours: 1), // no auto-flush in tests
  int maxRetries = 5,
  int maxQueueSize = 1000,
  DatacatStorage? storage,
  void Function(Object, List<DatacatEvent>)? onError,
  Future<String> Function()? getToken,
  Object? actorId = _noActor,
}) {
  return DatacatClient(
    config: DatacatConfig(
      endpoint: 'https://ingest.example.com/v1/events',
      getToken: getToken ?? () async => _makeJwt(exp: _futureEpoch(600)),
      actorId: identical(actorId, _noActor) ? 'user-123' : actorId as String?,
      tenantId: 'tenant-42',
      batchSize: batchSize,
      flushInterval: flushInterval,
      maxRetries: maxRetries,
      maxQueueSize: maxQueueSize,
      httpClient: httpClient,
      storage: storage ?? InMemoryStorage(),
      onError: onError,
    ),
  );
}

/// Creates a minimal, structurally valid JWT with the given exp epoch.
String _makeJwt({required int exp}) {
  String b64(Map<String, dynamic> m) =>
      base64Url.encode(utf8.encode(jsonEncode(m))).replaceAll('=', '');
  final header = b64({'alg': 'EdDSA', 'typ': 'JWT'});
  final payload = b64({'sub': 'user-123', 'actor_id': 'user-123', 'exp': exp});
  return '$header.$payload.fakesig';
}

int _futureEpoch(int seconds) =>
    DateTime.now().millisecondsSinceEpoch ~/ 1000 + seconds;

int _pastEpoch(int seconds) =>
    DateTime.now().millisecondsSinceEpoch ~/ 1000 - seconds;

/// Decodes the JSON body sent to the mock server.
Map<String, dynamic> _decodeBody(http.Request req) =>
    jsonDecode(req.body) as Map<String, dynamic>;

List<Map<String, dynamic>> _events(Map<String, dynamic> body) =>
    (body['events'] as List).cast<Map<String, dynamic>>();

// ── Tests ─────────────────────────────────────────────────────────────────────

void main() {
  group('Wire format', () {
    test('batch body contains events array with required fields', () async {
      late Map<String, dynamic> capturedBody;

      final mock = MockClient((req) async {
        capturedBody = _decodeBody(req);
        return http.Response('{"received":1}', 202);
      });

      final client = _makeClient(httpClient: mock);
      // Wait for session init
      await Future.delayed(Duration.zero);

      client.track('page_viewed', {'page': 'home'});
      await client.flush();

      expect(capturedBody.containsKey('events'), isTrue);
      final events = _events(capturedBody);
      expect(events, hasLength(1));

      final e = events.first;
      expect(e['event_id'], isA<String>());
      expect(e['event_name'], equals('page_viewed'));
      expect(e['actor_id'], equals('user-123'));
      expect(e['session_id'], isA<String>());
      expect(e['timestamp_client'], isA<String>());
      expect(e['tenant_id'], equals('tenant-42'));
      expect(e['properties'], equals({'page': 'home'}));

      await client.dispose();
    });

    test('timestamp_client is ISO-8601 UTC', () async {
      late Map<String, dynamic> capturedBody;

      final mock = MockClient((req) async {
        capturedBody = _decodeBody(req);
        return http.Response('{"received":1}', 202);
      });

      final client = _makeClient(httpClient: mock);
      await Future.delayed(Duration.zero);

      client.track('ts_test');
      await client.flush();

      final ts = _events(capturedBody).first['timestamp_client'] as String;
      // Must end in Z (UTC) and be parseable.
      expect(ts.endsWith('Z'), isTrue);
      expect(() => DateTime.parse(ts), returnsNormally);

      await client.dispose();
    });

    test('absent properties field defaults to empty map', () async {
      late Map<String, dynamic> capturedBody;

      final mock = MockClient((req) async {
        capturedBody = _decodeBody(req);
        return http.Response('{"received":1}', 202);
      });

      final client = _makeClient(httpClient: mock);
      await Future.delayed(Duration.zero);

      client.track('no_props');
      await client.flush();

      final props = _events(capturedBody).first['properties'];
      expect(props, equals(<String, dynamic>{}));

      await client.dispose();
    });

    test('no duplicate event_ids in a single batch', () async {
      late Map<String, dynamic> capturedBody;

      final mock = MockClient((req) async {
        capturedBody = _decodeBody(req);
        return http.Response('{"received":5}', 202);
      });

      final client = _makeClient(httpClient: mock);
      await Future.delayed(Duration.zero);

      for (var i = 0; i < 5; i++) {
        client.track('event_$i');
      }
      await client.flush();

      final ids =
          _events(capturedBody).map((e) => e['event_id'] as String).toList();
      expect(ids.toSet().length, equals(ids.length));

      await client.dispose();
    });
  });

  group('event_id and timestamp_client frozen on retry', () {
    test('event_id is preserved across retries', () async {
      final capturedIds = <String>[];
      var callCount = 0;

      final mock = MockClient((req) async {
        callCount++;
        final body = _decodeBody(req);
        for (final e in _events(body)) {
          capturedIds.add(e['event_id'] as String);
        }
        // Fail first call, succeed on second.
        if (callCount == 1) {
          return http.Response('{"error":"server error"}', 500);
        }
        return http.Response('{"received":1}', 202);
      });

      final client = _makeClient(
        httpClient: mock,
        maxRetries: 3,
        flushInterval: const Duration(milliseconds: 50),
      );
      await Future.delayed(Duration.zero);

      client.track('retry_event');

      // First flush (fails) then second flush (succeeds).
      await client.flush();
      await client.flush();

      expect(callCount, equals(2));
      expect(capturedIds, hasLength(2));
      // Both sends must use the exact same event_id.
      expect(capturedIds[0], equals(capturedIds[1]));

      await client.dispose();
    });

    test('timestamp_client is frozen on retry', () async {
      final capturedTimestamps = <String>[];
      var callCount = 0;

      final mock = MockClient((req) async {
        callCount++;
        for (final e in _events(_decodeBody(req))) {
          capturedTimestamps.add(e['timestamp_client'] as String);
        }
        if (callCount == 1) return http.Response('error', 500);
        return http.Response('{"received":1}', 202);
      });

      final client = _makeClient(httpClient: mock, maxRetries: 3);
      await Future.delayed(Duration.zero);

      client.track('ts_frozen');
      await client.flush();

      // Small delay to ensure wall-clock would differ if timestamp was regenerated.
      await Future.delayed(const Duration(milliseconds: 10));
      await client.flush();

      expect(callCount, equals(2));
      expect(capturedTimestamps, hasLength(2));
      expect(capturedTimestamps[0], equals(capturedTimestamps[1]));

      await client.dispose();
    });
  });

  group('Batching', () {
    test('flushes automatically when batchSize is reached', () async {
      var requestCount = 0;

      final mock = MockClient((req) async {
        requestCount++;
        return http.Response('{"received":3}', 202);
      });

      final client = _makeClient(
        httpClient: mock,
        batchSize: 3,
        flushInterval: const Duration(hours: 1),
      );
      await Future.delayed(Duration.zero);

      client.track('e1');
      client.track('e2');
      // Third event triggers automatic flush.
      client.track('e3');

      // Allow microtasks to complete.
      await Future.delayed(Duration.zero);

      expect(requestCount, equals(1));

      await client.dispose();
    });

    test('batch body wraps events in { events: [...] }', () async {
      late String rawBody;

      final mock = MockClient((req) async {
        rawBody = req.body;
        return http.Response('{"received":2}', 202);
      });

      final client = _makeClient(httpClient: mock, batchSize: 2);
      await Future.delayed(Duration.zero);

      client.track('a');
      client.track('b');
      await Future.delayed(Duration.zero);

      final decoded = jsonDecode(rawBody) as Map<String, dynamic>;
      expect(decoded.keys.toList(), contains('events'));
      expect(_events(decoded), hasLength(2));

      await client.dispose();
    });

    test('explicit flush sends remaining events', () async {
      var requestCount = 0;

      final mock = MockClient((req) async {
        requestCount++;
        return http.Response('{"received":2}', 202);
      });

      final client = _makeClient(
        httpClient: mock,
        batchSize: 100,
        flushInterval: const Duration(hours: 1),
      );
      await Future.delayed(Duration.zero);

      client.track('x');
      client.track('y');
      await client.flush();

      expect(requestCount, equals(1));

      await client.dispose();
    });
  });

  group('Token', () {
    test('Authorization header carries Bearer token', () async {
      late String capturedAuth;

      final mock = MockClient((req) async {
        capturedAuth = req.headers['authorization'] ?? '';
        return http.Response('{"received":1}', 202);
      });

      const jwt = 'header.payload.sig';
      final client = _makeClient(
        httpClient: mock,
        getToken: () async => jwt,
      );
      await Future.delayed(Duration.zero);

      client.track('auth_test');
      await client.flush();

      expect(capturedAuth, equals('Bearer $jwt'));

      await client.dispose();
    });

    test('token is refreshed on 401', () async {
      var tokenCallCount = 0;
      var requestCount = 0;

      // Return a token that expires far in the future so caching is stable.
      final tokens = [
        _makeJwt(exp: _futureEpoch(600)),
        _makeJwt(exp: _futureEpoch(600)),
      ];
      final capturedTokens = <String>[];

      final mock = MockClient((req) async {
        requestCount++;
        capturedTokens.add(req.headers['authorization'] ?? '');
        if (requestCount == 1) {
          return http.Response('{"error":"unauthorized"}', 401);
        }
        return http.Response('{"received":1}', 202);
      });

      final client = _makeClient(
        httpClient: mock,
        getToken: () async {
          final t = tokens[tokenCallCount % tokens.length];
          tokenCallCount++;
          return t;
        },
      );
      await Future.delayed(Duration.zero);

      client.track('token_refresh');
      await client.flush();

      // getToken was called at least twice (initial + refresh after 401).
      expect(tokenCallCount, greaterThanOrEqualTo(2));
      // Two HTTP requests were made.
      expect(requestCount, equals(2));

      await client.dispose();
    });

    test('token is renewed before expiry (within 30 s window)', () async {
      var tokenCallCount = 0;

      // Return an "almost expired" token first (exp - now < 30 s).
      final tokens = [
        _makeJwt(exp: _futureEpoch(10)), // will trigger proactive renewal
        _makeJwt(exp: _futureEpoch(600)),
      ];

      final mock = MockClient(
        (_) async => http.Response('{"received":1}', 202),
      );

      final client = _makeClient(
        httpClient: mock,
        getToken: () async {
          final t = tokens[tokenCallCount.clamp(0, tokens.length - 1)];
          tokenCallCount++;
          return t;
        },
      );
      await Future.delayed(Duration.zero);

      // First track — cache is empty → getToken called.
      client.track('pre_expiry_1');
      await client.flush();

      // Second track — almost-expired token → proactive renewal.
      client.track('pre_expiry_2');
      await client.flush();

      // Should have been called twice: initial + proactive renewal.
      expect(tokenCallCount, equals(2));

      await client.dispose();
    });
  });

  group('Retry behaviour', () {
    test('events are retried on 5xx', () async {
      var callCount = 0;

      final mock = MockClient((req) async {
        callCount++;
        if (callCount < 3) return http.Response('error', 500);
        return http.Response('{"received":1}', 202);
      });

      final client = _makeClient(httpClient: mock, maxRetries: 5);
      await Future.delayed(Duration.zero);

      client.track('retry_5xx');
      // Flush three times to exhaust failures and succeed.
      await client.flush();
      await client.flush();
      await client.flush();

      expect(callCount, equals(3));

      await client.dispose();
    });

    test('events are retried on 429', () async {
      var callCount = 0;
      final capturedIds = <String>[];

      final mock = MockClient((req) async {
        callCount++;
        for (final e in _events(_decodeBody(req))) {
          capturedIds.add(e['event_id'] as String);
        }
        if (callCount == 1) {
          return http.Response(
            '{"error":"rate limited"}',
            429,
            headers: {'retry-after': '0'},
          );
        }
        return http.Response('{"received":1}', 202);
      });

      final client = _makeClient(httpClient: mock, maxRetries: 3);
      await Future.delayed(Duration.zero);

      client.track('rate_limited');
      await client.flush();
      // Short wait for scheduled retry from retry-after header.
      await Future.delayed(const Duration(milliseconds: 50));

      expect(callCount, greaterThanOrEqualTo(2));
      // Both attempts must carry the same event_id.
      if (capturedIds.length >= 2) {
        expect(capturedIds[0], equals(capturedIds[1]));
      }

      await client.dispose();
    });

    test('events are abandoned on 400 and onError is called', () async {
      var callCount = 0;
      final droppedEvents = <DatacatEvent>[];

      final mock = MockClient((req) async {
        callCount++;
        return http.Response('{"error":"bad request"}', 400);
      });

      final client = _makeClient(
        httpClient: mock,
        maxRetries: 5,
        onError: (_, events) => droppedEvents.addAll(events),
      );
      await Future.delayed(Duration.zero);

      client.track('bad_event');
      await client.flush();

      // Exactly one request, no retries.
      expect(callCount, equals(1));
      expect(droppedEvents, hasLength(1));
      expect(droppedEvents.first.eventName, equals('bad_event'));

      await client.dispose();
    });

    test('events are dropped after maxRetries and onError is called', () async {
      var callCount = 0;
      final droppedEvents = <DatacatEvent>[];

      final mock = MockClient((req) async {
        callCount++;
        return http.Response('error', 500);
      });

      final client = _makeClient(
        httpClient: mock,
        maxRetries: 2,
        onError: (_, events) => droppedEvents.addAll(events),
      );
      await Future.delayed(Duration.zero);

      client.track('will_drop');

      // Flush maxRetries + 1 times.
      for (var i = 0; i <= 2; i++) {
        await client.flush();
      }

      expect(callCount, equals(3)); // initial + 2 retries
      expect(droppedEvents, hasLength(1));

      await client.dispose();
    });

    test('network error keeps events in queue for next flush', () async {
      var callCount = 0;
      late Map<String, dynamic> successBody;

      final mock = MockClient((req) async {
        callCount++;
        if (callCount == 1) throw Exception('network error');
        successBody = _decodeBody(req);
        return http.Response('{"received":1}', 202);
      });

      final client = _makeClient(httpClient: mock, maxRetries: 3);
      await Future.delayed(Duration.zero);

      client.track('network_fail');
      // First flush: network error.
      await client.flush();
      // Second flush: success.
      await client.flush();

      expect(callCount, equals(2));
      // The event must be present in the successful request.
      expect(_events(successBody), hasLength(1));

      await client.dispose();
    });
  });

  group('Session ID', () {
    test('session_id is persisted in storage and reused', () async {
      String? capturedSessionId;

      final mock = MockClient((req) async {
        capturedSessionId =
            _events(_decodeBody(req)).first['session_id'] as String?;
        return http.Response('{"received":1}', 202);
      });

      final storage = InMemoryStorage();
      final client1 = _makeClient(httpClient: mock, storage: storage);
      await Future.delayed(Duration.zero);

      client1.track('first');
      await client1.flush();
      final id1 = capturedSessionId;
      await client1.dispose();

      // New client sharing the same storage should reuse the session.
      final client2 = DatacatClient(
        config: DatacatConfig(
          endpoint: 'https://ingest.example.com/v1/events',
          getToken: () async => _makeJwt(exp: _futureEpoch(600)),
          actorId: 'user-123',
          batchSize: 100,
          flushInterval: const Duration(hours: 1),
          httpClient: mock,
          storage: storage,
        ),
      );
      await Future.delayed(Duration.zero);

      client2.track('second');
      await client2.flush();
      final id2 = capturedSessionId;
      await client2.dispose();

      expect(id1, isNotNull);
      expect(id1, equals(id2));
    });

    test('session_id override from config is used', () async {
      String? capturedSessionId;

      final mock = MockClient((req) async {
        capturedSessionId =
            _events(_decodeBody(req)).first['session_id'] as String?;
        return http.Response('{"received":1}', 202);
      });

      final client = DatacatClient(
        config: DatacatConfig(
          endpoint: 'https://ingest.example.com/v1/events',
          getToken: () async => _makeJwt(exp: _futureEpoch(600)),
          actorId: 'user-123',
          sessionId: 'my-custom-session',
          batchSize: 100,
          flushInterval: const Duration(hours: 1),
          httpClient: mock,
        ),
      );
      await Future.delayed(Duration.zero);

      client.track('custom_session');
      await client.flush();

      expect(capturedSessionId, equals('my-custom-session'));

      await client.dispose();
    });
  });

  group('Queue overflow', () {
    test('oldest events are dropped when maxQueueSize is exceeded', () async {
      final droppedEvents = <DatacatEvent>[];
      // Never succeeds — we just want to see overflow behaviour.
      final mock = MockClient(
        (_) async => http.Response('error', 500),
      );

      const maxQueue = 3;
      final client = _makeClient(
        httpClient: mock,
        maxQueueSize: maxQueue,
        batchSize: 100,
        flushInterval: const Duration(hours: 1),
        onError: (_, events) => droppedEvents.addAll(events),
      );
      await Future.delayed(Duration.zero);

      // Track maxQueue + 1 events — oldest should be dropped.
      for (var i = 0; i < maxQueue + 1; i++) {
        client.track('overflow_$i');
      }

      // The queue must not exceed maxQueueSize.
      // We can't inspect _queue directly, so we verify via onError being called.
      expect(droppedEvents, hasLength(1));
      expect(droppedEvents.first.eventName, equals('overflow_0'));

      await client.dispose();
    });
  });

  group('dispose', () {
    test('flush is called on dispose', () async {
      var requestCount = 0;

      final mock = MockClient((req) async {
        requestCount++;
        return http.Response('{"received":1}', 202);
      });

      final client = _makeClient(
        httpClient: mock,
        batchSize: 100,
        flushInterval: const Duration(hours: 1),
      );
      await Future.delayed(Duration.zero);

      client.track('before_dispose');
      await client.dispose();

      expect(requestCount, equals(1));
    });

    test('track is a no-op after dispose', () async {
      var requestCount = 0;

      final mock = MockClient((req) async {
        requestCount++;
        return http.Response('{"received":1}', 202);
      });

      final client = _makeClient(httpClient: mock);
      await Future.delayed(Duration.zero);

      await client.dispose();
      client.track('after_dispose');
      await Future.delayed(const Duration(milliseconds: 50));

      expect(requestCount, equals(0));
    });
  });

  group('TokenCache unit tests', () {
    test('decodes exp from JWT and triggers renewal before expiry', () async {
      var callCount = 0;
      final cache = TokenCache(
        getToken: () async {
          callCount++;
          return _makeJwt(exp: _futureEpoch(600));
        },
      );

      await cache.token();
      await cache.token(); // should be cached
      expect(callCount, equals(1));
    });

    test('invalidate forces a fresh token fetch', () async {
      var callCount = 0;
      final cache = TokenCache(
        getToken: () async {
          callCount++;
          return _makeJwt(exp: _futureEpoch(600));
        },
      );

      await cache.token();
      await cache.invalidate();
      await cache.token();
      expect(callCount, equals(2));
    });

    test('almost-expired token triggers proactive renewal', () async {
      var callCount = 0;
      final tokens = [
        _makeJwt(exp: _pastEpoch(5)), // already expired
        _makeJwt(exp: _futureEpoch(600)),
      ];
      final cache = TokenCache(
        getToken: () async => tokens[callCount++ < 1 ? 0 : 1],
      );

      await cache.token(); // first call
      await cache.token(); // should re-fetch because exp is in the past

      expect(callCount, equals(2));
    });
  });

  group('identify', () {
    test('event tracked after identify() carries the new actor/tenant',
        () async {
      late Map<String, dynamic> capturedBody;

      final mock = MockClient((req) async {
        capturedBody = _decodeBody(req);
        return http.Response('{"received":1}', 202);
      });

      final client = _makeClient(httpClient: mock);
      await Future.delayed(Duration.zero);

      client.identify(actorId: 'user-999', tenantId: 'tenant-777');
      client.track('after_identify');
      await client.flush();

      final e = _events(capturedBody).first;
      expect(e['actor_id'], equals('user-999'));
      expect(e['tenant_id'], equals('tenant-777'));

      await client.dispose();
    });

    test(
      'identify() enables tracking when no actorId was set in config',
      () async {
        var requestCount = 0;
        late Map<String, dynamic> capturedBody;

        final mock = MockClient((req) async {
          requestCount++;
          capturedBody = _decodeBody(req);
          return http.Response('{"received":1}', 202);
        });

        // No actor at construction (B2B: authentication happens later).
        final client = _makeClient(httpClient: mock, actorId: null);
        await Future.delayed(Duration.zero);

        client.identify(actorId: 'late-user');
        client.track('post_auth_event');
        await client.flush();

        expect(requestCount, equals(1));
        final e = _events(capturedBody).first;
        expect(e['actor_id'], equals('late-user'));
        // tenantId not provided to identify() → absent from wire format.
        expect(e.containsKey('tenant_id'), isFalse);

        await client.dispose();
      },
    );

    test(
      'track() without any actor identity drops the event and calls onError',
      () async {
        var requestCount = 0;
        final errors = <Object>[];

        final mock = MockClient((req) async {
          requestCount++;
          return http.Response('{"received":1}', 202);
        });

        final client = _makeClient(
          httpClient: mock,
          actorId: null, // no actor in config, identify() never called
          onError: (error, _) => errors.add(error),
        );
        await Future.delayed(Duration.zero);

        client.track('orphan_event');
        await client.flush();

        // No HTTP request, event dropped, onError invoked, no crash.
        expect(requestCount, equals(0));
        expect(errors, hasLength(1));
        expect(errors.first, isA<StateError>());

        await client.dispose();
      },
    );
  });
}
