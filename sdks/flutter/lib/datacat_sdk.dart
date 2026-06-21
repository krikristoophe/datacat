/// Datacat analytics SDK — pure-Dart core.
///
/// Import this library from any Dart or Flutter project:
/// ```dart
/// import 'package:datacat_sdk/datacat_sdk.dart';
/// ```
///
/// For Flutter lifecycle integration (flush on pause/detach) and
/// persistent session storage, see the README.
library;

export 'src/client/client.dart'
    show DatacatClient, DatacatConfig, HttpException;
export 'src/core/event.dart' show DatacatEvent;
export 'src/core/storage.dart' show DatacatStorage, InMemoryStorage;
export 'src/token/token_cache.dart' show TokenCache;
