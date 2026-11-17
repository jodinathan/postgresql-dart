import 'dart:async';
import 'dart:convert';
import 'dart:typed_data';

import 'package:async/async.dart';
import 'package:postgres/messages.dart';
import 'package:postgres/postgres.dart';
import 'package:postgres/src/protocol/buffer.dart';
import 'package:postgres/src/protocol/message_window.dart';
import 'package:stream_channel/stream_channel.dart';
import 'package:test/test.dart';

import 'support/docker.dart';

void main() {
  group('LogicalDecodingMessage bytes', () {
    Uint8List messageBytes({
      required bool transactional,
      required LSN lsn,
      required String prefix,
      required List<int> content,
    }) {
      final writer = PgByteDataWriter(encoding: utf8);
      writer
        ..writeUint8('M'.codeUnitAt(0))
        ..writeUint8(transactional ? 1 : 0)
        ..writeUint64(lsn.value)
        ..writeEncodedString(writer.encodeString(prefix))
        ..writeUint32(content.length)
        ..write(content);
      return writer.toBytes();
    }

    Uint8List copyDataFrame(Uint8List logicalMessage) {
      final xLogData = PgByteDataWriter(encoding: utf8)
        ..writeUint8(ReplicationMessageId.xLogData)
        ..writeUint64(0x16B3748) // walStart
        ..writeUint64(0x16B3800) // walEnd
        ..writeUint64(0) // time
        ..write(logicalMessage);
      return CopyDataMessage(xLogData.toBytes()).asBytes(encoding: utf8);
    }

    test('decodes flags, lsn, prefix and content', () async {
      final cases = [
        (true, LSN.fromString('0/16B3748'), 'oni-barrier', <int>[]),
        (false, LSN.fromString('1/2A'), 'p', utf8.encode('n1')),
      ];
      for (final (transactional, lsn, prefix, content) in cases) {
        final bytes = messageBytes(
          transactional: transactional,
          lsn: lsn,
          prefix: prefix,
          content: content,
        );
        final matcher = isA<LogicalDecodingMessage>()
            .having((m) => m.transactional, 'transactional', transactional)
            .having((m) => m.lsn, 'lsn', lsn)
            .having((m) => m.prefix, 'prefix', prefix)
            .having((m) => m.content, 'content', content)
            .having(
              (m) => m.baseMessage,
              'baseMessage',
              LogicalReplicationMessageTypes.message,
            );

        final parsed = await Stream.value(
          copyDataFrame(bytes),
        ).transform(BytesToMessageParser(CodecContext.withDefaults())).single;
        expect(
          parsed,
          isA<XLogDataLogicalMessage>().having(
            (m) => m.message,
            'message',
            matcher,
          ),
        );

        final reader = PgByteDataReader(
          codecContext: CodecContext.withDefaults(),
        )..add(bytes);
        expect(
          // ignore: deprecated_member_use_from_same_package
          tryParseLogicalReplicationMessage(reader, bytes.length),
          matcher,
        );
      }
    });
  });

  withPostgresServer('ReplicationStream', initSqls: replicationSchemaInit, (
    server,
  ) {
    late final Connection replicationConn;
    late final Connection changesConn;
    late final ReplicationStream stream;
    late final StreamQueue<ReplicationMessage> messages;

    Future<LogicalReplicationMessage> nextLogical() async {
      while (true) {
        final message = await messages.next.timeout(Duration(seconds: 10));
        // Keepalives arrive at any time, and RelationMessage only appears
        // before the first change of a relation.
        if (message is XLogDataLogicalMessage &&
            message.message is! RelationMessage) {
          return message.message;
        }
      }
    }

    Future<List<LogicalReplicationMessage>> nextTransaction() async {
      final transaction = <LogicalReplicationMessage>[];
      while (transaction.lastOrNull is! CommitMessage) {
        transaction.add(await nextLogical());
      }
      return transaction;
    }

    setUpAll(() async {
      replicationConn = await _openReplicationConnection(server);
      changesConn = await Connection.open(await server.endpoint());

      await changesConn.execute('CREATE SCHEMA test');
      await changesConn.execute(
        'CREATE TABLE test.items (id bigint PRIMARY KEY, value text)',
      );
      await changesConn.execute(
        'CREATE PUBLICATION items_pub FOR TABLE test.items (id)',
      );

      final lsn = await _createSlot(replicationConn, 's');
      stream = await replicationConn.startReplication(
        _startStatement('s', lsn),
      );
      messages = StreamQueue(stream.messages);
    });

    tearDownAll(() async {
      await stream.stop();
      await messages.cancel();
      await replicationConn.close();
      await changesConn.close();
    });

    test('delivers Begin, Insert and Commit of another connection', () async {
      await changesConn.execute('INSERT INTO test.items VALUES (1, \'one\')');

      expect(await nextTransaction(), [
        isA<BeginMessage>(),
        isA<InsertMessage>().having(
          (m) => m.tuple.columns.map((c) => c.value),
          'published columns',
          ['1'],
        ),
        isA<CommitMessage>(),
      ]);
    });

    test('decodes a non-transactional M message', () async {
      await changesConn.execute(
        "SELECT pg_logical_emit_message(false, 'oni-barrier', 'n1')",
      );

      expect(
        await nextLogical(),
        isA<LogicalDecodingMessage>()
            .having((m) => m.transactional, 'transactional', false)
            .having((m) => m.prefix, 'prefix', 'oni-barrier')
            .having((m) => m.content, 'content', utf8.encode('n1')),
      );
    });

    test(
      'decodes a transactional M message between Begin and Commit',
      () async {
        await changesConn.runTx((tx) async {
          await tx.execute('INSERT INTO test.items VALUES (2, \'two\')');
          await tx.execute(
            "SELECT pg_logical_emit_message(true, 'oni-barrier', 'n2')",
          );
        });

        expect(await nextTransaction(), [
          isA<BeginMessage>(),
          isA<InsertMessage>(),
          isA<LogicalDecodingMessage>()
              .having((m) => m.transactional, 'transactional', true)
              .having((m) => m.prefix, 'prefix', 'oni-barrier')
              .having((m) => m.content, 'content', utf8.encode('n2')),
          isA<CommitMessage>(),
        ]);
      },
    );

    test('advances confirmed_flush_lsn after setFlushedLsn', () async {
      await changesConn.execute('INSERT INTO test.items VALUES (3, \'three\')');
      final commit = (await nextTransaction()).last as CommitMessage;
      final flushed = commit.transactionEndLSN;
      expect(stream.lastReceivedLsn.value, greaterThanOrEqualTo(flushed.value));

      stream.setFlushedLsn(flushed);
      stream.setFlushedLsn(LSN(0));
      await stream.forceUpdateStatus();

      final deadline = DateTime.now().add(Duration(seconds: 2));
      LSN? confirmed;
      while (DateTime.now().isBefore(deadline)) {
        final result = await changesConn.execute(
          Sql.named(
            'SELECT confirmed_flush_lsn::text FROM pg_replication_slots '
            'WHERE slot_name = @slot',
          ),
          parameters: {'slot': 's'},
        );
        confirmed = LSN.fromString(result.single.single as String);
        if (confirmed.value >= flushed.value) break;
        await Future<void>.delayed(Duration(milliseconds: 100));
      }
      expect(confirmed, flushed);
    });

    test('refuses statements while streaming', () async {
      final refusal = throwsA(
        isA<PgException>().having(
          (e) => e.message,
          'message',
          contains('streaming replication'),
        ),
      );
      await expectLater(replicationConn.execute('SELECT 1'), refusal);
      await expectLater(replicationConn.prepare('SELECT 1'), refusal);
      expect(() => replicationConn.runTx((_) async {}), refusal);
      await expectLater(
        replicationConn.startReplication(_startStatement('s', '0/0')),
        refusal,
      );

      await changesConn.execute('INSERT INTO test.items VALUES (4, \'four\')');
      expect(await nextTransaction(), [
        isA<BeginMessage>(),
        isA<InsertMessage>(),
        isA<CommitMessage>(),
      ]);
    });

    test('refuses a statement queued before CopyBoth', () async {
      final conn = await _openReplicationConnection(server);
      final lsn = await _createSlot(conn, 'queued');

      final started = conn.startReplication(_startStatement('queued', lsn));
      final queued = conn.execute('SELECT 1');

      await expectLater(
        queued,
        throwsA(
          isA<PgException>().having(
            (e) => e.message,
            'message',
            contains('streaming replication'),
          ),
        ),
      );
      final queuedStream = await started;
      await queuedStream.stop();
      await conn.close();
    });

    test('stop returns the connection to normal use', () async {
      final conn = await _openReplicationConnection(server);
      final lsn = await _createSlot(conn, 'stopped');
      final stopped = await conn.startReplication(
        _startStatement('stopped', lsn),
      );
      final delivered = stopped.messages.toList();

      await stopped.stop();
      await expectLater(delivered, completes);
      await stopped.stop();

      final system = await conn.execute('IDENTIFY_SYSTEM');
      expect(system.single, hasLength(4));
      await conn.close();
    });

    test('close without force stops the stream first', () async {
      final conn = await _openReplicationConnection(server);
      final lsn = await _createSlot(conn, 'closed');
      final closed = await conn.startReplication(
        _startStatement('closed', lsn),
      );
      final delivered = closed.messages.toList();

      final watch = Stopwatch()..start();
      await conn.close();
      expect(watch.elapsed, lessThan(Duration(seconds: 5)));
      await expectLater(delivered, completes);

      final reopened = await _openReplicationConnection(server);
      final reopenedLsn = await _createSlot(reopened, 'reopened');
      final reopenedStream = await reopened.startReplication(
        _startStatement('reopened', reopenedLsn),
      );
      await reopenedStream.stop();
      await reopened.close();
    });

    test('answers keepalives that ask for a reply', () async {
      await changesConn.execute("ALTER SYSTEM SET wal_sender_timeout = '2s'");
      await changesConn.execute('SELECT pg_reload_conf()');
      addTearDown(() async {
        await changesConn.execute('ALTER SYSTEM RESET wal_sender_timeout');
        await changesConn.execute('SELECT pg_reload_conf()');
      });

      final conn = await _openReplicationConnection(server);
      final lsn = await _createSlot(conn, 'keepalive');
      final keepalive = await conn.startReplication(
        _startStatement('keepalive', lsn),
      );
      final received = <ReplicationMessage>[];
      Object? streamError;
      var streamDone = false;
      final subscription = keepalive.messages.listen(
        received.add,
        onError: (Object e) => streamError = e,
        onDone: () => streamDone = true,
      );
      addTearDown(subscription.cancel);

      await Future<void>.delayed(Duration(seconds: 6));

      expect(streamError, isNull);
      expect(streamDone, isFalse);
      expect(conn.isOpen, isTrue);
      expect(
        received,
        contains(
          isA<PrimaryKeepAliveMessage>().having(
            (m) => m.mustReply,
            'mustReply',
            isTrue,
          ),
        ),
      );
      await keepalive.stop();
      await conn.close();
    });
  });

  withPostgresServer(
    'ReplicationStream refusals',
    initSqls: replicationSchemaInit,
    (server) {
      test('startReplication without a replication mode throws', () async {
        final sent = <Message>[];
        final conn = await Connection.open(
          await server.endpoint(),
          settings: ConnectionSettings(
            transformer: StreamChannelTransformer(
              StreamTransformer.fromHandlers(),
              StreamSinkTransformer.fromHandlers(
                handleData: (message, sink) {
                  sent.add(message);
                  sink.add(message);
                },
              ),
            ),
          ),
        );
        sent.clear();

        await expectLater(
          conn.startReplication(_startStatement('missing', '0/0')),
          throwsA(
            isA<PgException>().having(
              (e) => e.message,
              'message',
              contains('replication mode'),
            ),
          ),
        );
        expect(sent, isEmpty);

        expect(await conn.execute('SELECT 1'), [
          [1],
        ]);
        await conn.close();
      });

      test(
        'startReplication on a missing slot fails with the server error',
        () async {
          final conn = await _openReplicationConnection(server);

          await expectLater(
            conn.startReplication(_startStatement('missing', '0/0')),
            throwsA(
              isA<ServerException>().having(
                (e) => e.message,
                'message',
                contains('missing'),
              ),
            ),
          );

          final system = await conn.execute('IDENTIFY_SYSTEM');
          expect(system.single, hasLength(4));
          await conn.close();
        },
      );

      test('startReplication on a pooled connection throws', () async {
        final pool = Pool.withEndpoints([await server.endpoint()]);
        addTearDown(pool.close);

        await pool.withConnection((conn) async {
          expect(
            () => conn.startReplication(_startStatement('missing', '0/0')),
            throwsUnsupportedError,
          );
        });
      });
    },
  );
}

Future<Connection> _openReplicationConnection(PostgresServer server) async {
  return Connection.open(
    Endpoint(
      host: 'localhost',
      database: 'postgres',
      username: 'replication',
      password: 'replication',
      port: await server.port,
    ),
    settings: ConnectionSettings(
      replicationMode: ReplicationMode.logical,
      queryMode: QueryMode.simple,
    ),
  );
}

/// Creates a temporary pgoutput slot and returns its consistent point.
Future<String> _createSlot(Connection conn, String slot) async {
  final result = await conn.execute(
    'CREATE_REPLICATION_SLOT $slot TEMPORARY LOGICAL pgoutput '
    'NOEXPORT_SNAPSHOT',
  );
  return result.single[1] as String;
}

String _startStatement(String slot, String lsn) =>
    'START_REPLICATION SLOT $slot LOGICAL $lsn '
    "(proto_version '1', publication_names 'items_pub', messages 'true')";
