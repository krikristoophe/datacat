/// Immutable representation of a single analytics event.
///
/// Both [eventId] and [timestampClient] are frozen at construction time and
/// MUST be reused verbatim on every retry — never regenerated.
final class DatacatEvent {
  DatacatEvent({
    required this.eventId,
    required this.eventName,
    required this.actorId,
    required this.sessionId,
    required this.timestampClient,
    this.tenantId,
    this.properties = const {},
  });

  /// UUID v4, generated once at creation. Idempotence key.
  final String eventId;

  /// Free-form event name, 1–200 characters.
  final String eventName;

  /// Persistent identity of the actor.
  final String actorId;

  /// Session identifier, generated and persisted by the SDK.
  final String sessionId;

  /// Client-side timestamp, frozen at creation (RFC-3339 / ISO-8601 UTC).
  final String timestampClient;

  /// Optional tenant identifier for multi-tenant B2B scenarios.
  final String? tenantId;

  /// Arbitrary event properties.
  ///
  /// IMPORTANT: never include sensitive data (PII, credentials, health data)
  /// in properties — they are stored in plain text and may appear in logs.
  final Map<String, dynamic> properties;

  Map<String, dynamic> toJson() {
    return <String, dynamic>{
      'event_id': eventId,
      'event_name': eventName,
      'actor_id': actorId,
      'session_id': sessionId,
      'timestamp_client': timestampClient,
      if (tenantId != null) 'tenant_id': tenantId,
      'properties': properties,
    };
  }
}
