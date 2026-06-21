import 'dart:convert';

/// Manages JWT token retrieval, caching, and renewal.
///
/// The token is renewed:
/// - proactively, ~30 s before expiry (to avoid sending an expired token);
/// - reactively, on every 401 response.
///
/// The token is never stored in the SDK source or shipped in the binary.
final class TokenCache {
  TokenCache({required this.getToken});

  /// Callback supplied by the application that returns a fresh JWT.
  final Future<String> Function() getToken;

  static const _renewBeforeSeconds = 30;

  String? _cachedToken;
  int? _expEpochSeconds;

  /// Returns a valid token, fetching a new one if the cache is empty or the
  /// token is about to expire.
  Future<String> token() async {
    if (_needsRenewal()) {
      await _refresh();
    }
    return _cachedToken!;
  }

  /// Unconditionally refreshes the token (called on 401).
  Future<void> invalidate() async {
    _cachedToken = null;
    _expEpochSeconds = null;
    await _refresh();
  }

  bool _needsRenewal() {
    if (_cachedToken == null || _expEpochSeconds == null) return true;
    final nowSeconds = DateTime.now().millisecondsSinceEpoch ~/ 1000;
    return nowSeconds >= _expEpochSeconds! - _renewBeforeSeconds;
  }

  Future<void> _refresh() async {
    final jwt = await getToken();
    _cachedToken = jwt;
    _expEpochSeconds = _parseExp(jwt);
  }

  /// Decodes the JWT payload (Base64url, no signature verification) and
  /// extracts the `exp` claim. Returns `null` on any parse failure, which
  /// will cause the next call to fetch a fresh token.
  static int? _parseExp(String jwt) {
    final parts = jwt.split('.');
    if (parts.length < 2) return null;
    try {
      // Base64url → Base64 (add padding as needed).
      var payload = parts[1].replaceAll('-', '+').replaceAll('_', '/');
      switch (payload.length % 4) {
        case 2:
          payload += '==';
        case 3:
          payload += '=';
      }
      final decoded = utf8.decode(base64.decode(payload));
      final claims = jsonDecode(decoded) as Map<String, dynamic>;
      final exp = claims['exp'];
      if (exp is int) return exp;
      if (exp is double) return exp.toInt();
      return null;
    } catch (_) {
      return null;
    }
  }
}
