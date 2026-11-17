// TODO: these types could move to a common "connection_config.dart" file

import '../../postgres.dart' show Connection;
import '../protocol/messages/logical_replication_messages.dart'
    show XLogDataLogicalMessage;
import '../protocol/messages/server_messages.dart'
    show PrimaryKeepAliveMessage, XLogDataMessage;
import '../protocol/messages/shared_messages.dart' show ReplicationMessage;
import '../types/types.dart' show LSN;

/// Streaming Replication Protocol Options
///
/// [physical] or [logical] are used to start the connection a streaming
/// replication mode.
///
/// See [Protocol Replication][] for more details.
///
/// [Protocol Replication]: https://www.postgresql.org/docs/current/protocol-replication.html
enum ReplicationMode {
  physical('true'),
  logical('database'),
  none('false');

  final String value;

  const ReplicationMode(this.value);
}

/// Settings of a [ReplicationStream].
class ReplicationStreamSettings {
  /// How often a Standby Status Update is sent while the stream is open.
  /// Defaults to 10 seconds, the server's `wal_receiver_status_interval`.
  final Duration? statusInterval;

  /// Whether a [PrimaryKeepAliveMessage] asking for a reply is answered with a
  /// Standby Status Update right away. Defaults to `true`.
  final bool? replyToKeepAlive;

  const ReplicationStreamSettings({this.statusInterval, this.replyToKeepAlive});
}

/// The CopyBoth stream started by [Connection.startReplication].
abstract class ReplicationStream {
  /// The server messages: [XLogDataMessage] (an [XLogDataLogicalMessage] when
  /// the payload is a decoded logical replication message) and
  /// [PrimaryKeepAliveMessage]. Single subscription; closes when the stream
  /// stops or the connection closes, with the error that closed it.
  Stream<ReplicationMessage> get messages;

  /// The highest WAL end received so far.
  LSN get lastReceivedLsn;

  /// Records [lsn] as flushed by the client; sent with the next status update.
  ///
  /// The position never moves back: a value lower than the current one is
  /// ignored.
  void setFlushedLsn(LSN lsn);

  /// Records [lsn] as applied by the client; sent with the next status update.
  ///
  /// The position never moves back: a value lower than the current one is
  /// ignored.
  void setAppliedLsn(LSN lsn);

  /// Sends a Standby Status Update now, asking the server to reply with a
  /// keepalive when [requestReply] is `true`.
  Future<void> forceUpdateStatus({bool requestReply = false});

  /// Ends the stream with CopyDone and waits until the server finishes the
  /// command; afterwards the connection accepts statements again.
  Future<void> stop();
}
