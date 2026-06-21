/// Abstraction for persistent key-value storage used to persist the session ID.
///
/// The default in-memory implementation is suitable for tests and for apps that
/// do not need cross-restart session continuity. For Flutter apps, provide an
/// implementation backed by `shared_preferences` (see README for a snippet).
abstract interface class DatacatStorage {
  /// Returns the value for [key], or `null` if not present.
  Future<String?> read(String key);

  /// Persists [value] under [key].
  Future<void> write(String key, String value);
}

/// In-memory storage implementation (default, no persistence across restarts).
final class InMemoryStorage implements DatacatStorage {
  final Map<String, String> _store = {};

  @override
  Future<String?> read(String key) async => _store[key];

  @override
  Future<void> write(String key, String value) async {
    _store[key] = value;
  }
}
