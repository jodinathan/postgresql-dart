import 'dart:convert';
import 'dart:typed_data';

import 'package:postgres/messages.dart';
import 'package:postgres/postgres.dart';
import 'package:postgres/src/protocol/buffer.dart';
import 'package:postgres/src/protocol/message_window.dart';
import 'package:test/test.dart';

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
}
