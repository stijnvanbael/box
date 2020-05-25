import 'package:postgres/postgres.dart';

import '../core.dart';

class PostgresBox extends Box {
  final PostgreSQLConnection _connection;

  PostgresBox(
    String hostname,
    Registry registry,
    String database, {
    int port = 5432,
    String username = 'postgres',
    String password = 'postgres',
  })  : _connection = PostgreSQLConnection(
          hostname,
          port,
          database,
          username: username,
          password: password,
        ),
        super(registry);

  @override
  Future close() => _connection.close();

  @override
  Future deleteAll<T>([Type type]) {
    // TODO: implement deleteAll
    throw UnimplementedError();
  }

  @override
  Future<T> find<T>(key, [Type type]) {
    // TODO: implement find
    throw UnimplementedError();
  }

  @override
  SelectStep select(List<Field> fields) {
    // TODO: implement select
    throw UnimplementedError();
  }

  @override
  QueryStep<T> selectFrom<T>([Type type]) {
    // TODO: implement selectFrom
    throw UnimplementedError();
  }

  @override
  Future store(dynamic entity) {
    // TODO: implement store
    throw UnimplementedError();
  }

  Future<PostgreSQLConnection> get _openConnection async {
    if(_connection.isClosed) {
      await _connection.open();
    }
    return _connection;
  }
}
