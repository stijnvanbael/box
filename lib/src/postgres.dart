import 'dart:convert';

import 'package:postgres/postgres.dart';
import 'package:recase/recase.dart';

import '../core.dart';
import 'pattern_matcher.dart';

enum ObjectRepresentation {
  json,

  // Not fully supported by the driver yet
  typesAndArrays
}

class PostgresBox extends Box {
  final PostgreSQLConnection _connection;
  final ObjectRepresentation objectRepresentation;

  PostgresBox(
    String hostname,
    Registry registry, {
    required String database,
    int port = 5432,
    String username = 'postgres',
    String password = 'postgres',
    this.objectRepresentation = ObjectRepresentation.json,
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
  Future deleteAll<T>([Type? type]) async {
    var connection = await _openConnection;
    var entitySupport = registry.lookup(type ?? T);
    var tableName = _snakeCase(entitySupport.name);
    return connection.execute('DELETE FROM "$tableName"');
  }

  @override
  Future<T?> find<T>(key, [Type? type]) async {
    var connection = await _openConnection;
    var entitySupport = registry.lookup(type ?? T);
    var tableName = _snakeCase(entitySupport.name);
    var conditions = entitySupport.keyFields
        .map((field) => '${_snakeCase(field)} = @$field')
        .join(' AND ');
    var values = (key is Map ? key : {entitySupport.keyFields.first: key})
        as Map<String, dynamic>;
    var results = await connection.mappedResultsQuery(
        'SELECT * FROM "$tableName" WHERE $conditions',
        substitutionValues: values);
    if (results.isNotEmpty) {
      return _mapRow<T>(results.first[tableName]!, entitySupport, type, [], []);
    }
    return null;
  }

  Stream<T> _query<T>(
    String conditions,
    Map<String, dynamic> bindings,
    Map<String, String> order,
    Type? type,
    int limit,
    int offset,
    List<Field> selectFields,
    Map<_Table, String> joins,
  ) async* {
    var connection = await _openConnection;
    var entitySupport = registry.lookup(type ?? T);
    var tableName = _snakeCase(entitySupport.name);
    var orderClause = order.isNotEmpty
        ? 'ORDER BY ${order.entries.map((e) => '${_snakeCase(e.key)} ${e.value}').join(', ')}'
        : '';
    var sql = 'SELECT * FROM "$tableName"'
        '${joins.isNotEmpty ? joins.entries.map((e) => ' INNER JOIN ${e.key.name} ON ${e.value}').join(' ') : ''}'
        '${conditions.isNotEmpty ? ' WHERE $conditions ' : ' '}'
        '$orderClause '
        'LIMIT $limit OFFSET $offset';
    try {
      var results = await connection.mappedResultsQuery(sql,
          substitutionValues: bindings);
      for (var result in results) {
        yield _mapRow<T>(
            result[tableName], entitySupport, type, selectFields, joins.keys);
      }
    } catch (e) {
      print(
          'Error executing SQL: $sql\n  Bindings: $bindings\n  Error message: $e');
      rethrow;
    }
  }

  T _mapRow<T>(
    Map<String, dynamic>? row,
    EntitySupport entitySupport,
    Type? type,
    List<Field> selectFields,
    Iterable<_Table> joinTables,
  ) {
    if (joinTables.isEmpty) {
      var converted = _convertResult<T>(row, type);
      if (selectFields.isEmpty) {
        return entitySupport.deserialize(converted);
      } else {
        return _mapFields(converted, selectFields) as T;
      }
    } else {
      return {
        entitySupport.name: _mapRow(row, entitySupport, type, selectFields, []),
        ...{
          for (var table in joinTables)
            registry.lookup(table.type).name: _mapRow(
                row, registry.lookup(table.type), table.type, selectFields, [])
        }
      } as T;
    }
  }

  Map<String, dynamic> _mapFields(
      Map<String, dynamic> converted, List<Field> selectFields) {
    return {
      for (var field in selectFields) field.alias: field.resolve(converted)
    };
  }

  dynamic _convertResult<T>(dynamic value, [Type? type]) {
    if (value is Map) {
      return value.map((key, value) => MapEntry(
          _camelCase(key), _deserialize<T>(_camelCase(key), value, type)));
    } else {
      return value;
    }
  }

  dynamic _deserialize<T>(String name, dynamic value, [Type? type]) {
    if (objectRepresentation == ObjectRepresentation.json && value is String) {
      var entitySupport = registry.lookup<T>(type);
      if (entitySupport.fieldTypes[name] != String) {
        return _fromJson(jsonDecode(value), entitySupport.fieldTypes[name]!);
      }
    }
    return _convertResult<T>(value, type);
  }

  @override
  SelectStep select(List<Field> fields) => _SelectStep(this, fields);

  @override
  QueryStep<T> selectFrom<T>([Type? type, String? alias]) =>
      _QueryStep<T>(this, type, []);

  @override
  Future<K> store<K>(dynamic entity) async {
    var connection = await _openConnection;
    var entitySupport = registry.lookup(entity.runtimeType);
    var tableName = _snakeCase(entitySupport.name);
    var fieldNames = entitySupport.fields.map((field) => _snakeCase(field));
    var fieldValues = _addEntityValues('', {}, entity);
    var statement =
        'INSERT INTO "$tableName"(${fieldNames.map((field) => '"$field"').join(', ')}) '
        'VALUES(${entitySupport.fields.map((field) => _fieldExpression(field, entitySupport.getFieldValue(field, entity))).join(', ')})';
    await connection.execute(statement, substitutionValues: fieldValues);
    return keyOf(entity);
  }

  String? _fieldExpression(String field, dynamic value) {
    var fieldExpressionMatcher = matcher<dynamic, String>()
        .whenNull((v) => 'NULL')
        .when(
            any([
              typeIs<String>(),
              typeIs<num>(),
              typeIs<DateTime>(),
              typeIs<bool>()
            ]),
            (v) => '@$field')
        .whenIs<Iterable>((v) => _arrayExpression(v, field))
        .otherwise((v) => _entityExpression(v, field));
    return fieldExpressionMatcher.apply(value);
  }

  String _arrayExpression(Iterable iterable, String field) {
    var index = 0;
    if (objectRepresentation == ObjectRepresentation.typesAndArrays) {
      return 'ARRAY[${iterable.map((e) => _fieldExpression(field + '_${index++}', e)).join(', ')}]';
    } else {
      return '@$field';
    }
  }

  String _entityExpression(dynamic value, String field) {
    var entitySupport = registry.lookup(value.runtimeType);
    var fieldExpressions = entitySupport.fields.map((f) =>
        _fieldExpression('${field}_$f', entitySupport.getFieldValue(f, value)));
    if (objectRepresentation == ObjectRepresentation.typesAndArrays) {
      return 'ROW(${fieldExpressions.join(', ')})';
    } else {
      return '@$field';
    }
  }

  Map<String, dynamic> _addEntityValues(
      String prefix, Map<String, dynamic> values, dynamic entity) {
    var entitySupport = registry.lookup(entity.runtimeType);
    for (var field in entitySupport.fields) {
      var value = entitySupport.getFieldValue(field, entity);
      _addFieldValue(prefix + field, values, value);
    }
    return values;
  }

  void _addFieldValue(
      String prefix, Map<String, dynamic> values, dynamic value) {
    matcher<dynamic, void>()
        .whenNull((v) => values[prefix] = null)
        .when(
            any([
              typeIs<String>(),
              typeIs<num>(),
              typeIs<DateTime>(),
              typeIs<bool>()
            ]),
            (v) => values[prefix] = v)
        .whenIs<Iterable>((v) => _addArrayValues(prefix, values, v))
        .otherwise((v) =>
            objectRepresentation == ObjectRepresentation.typesAndArrays
                ? _addEntityValues(prefix + '_', values, v)
                : values[prefix] = jsonEncode(_toJson(v)))
        .apply(value);
  }

  void _addArrayValues(
      String prefix, Map<String, dynamic> values, Iterable iterable) {
    var index = 0;
    if (objectRepresentation == ObjectRepresentation.typesAndArrays) {
      for (var value in iterable) {
        _addFieldValue('${prefix}_${index++}', values, value);
      }
    } else {
      _addFieldValue(prefix, values, jsonEncode(_toJson(iterable)));
    }
  }

  Future<PostgreSQLConnection> get _openConnection async {
    if (_connection.isClosed) {
      await _connection.open();
      _createIndexes();
    }
    return _connection;
  }

  dynamic _toJson(dynamic object) {
    return matcher<dynamic, dynamic>()
        .whenNull((v) => null)
        .whenIs<Map>(
            (o) => o.map((key, value) => MapEntry(key, _toJson(value))))
        .whenIs<Iterable>((o) => o.map((value) => _toJson(value)).toList())
        .whenIs<DateTime>((o) => o.toIso8601String())
        .when(any([typeIs<String>(), typeIs<num>(), typeIs<bool>()]), (v) => v)
        .otherwise((input) {
      var entitySupport = registry.lookup(object.runtimeType);
      return entitySupport != null
          ? entitySupport.serialize(input)
          : input.toJson();
    }).apply(object);
  }

  dynamic _fromJson(dynamic json, Type type) {
    return matcher<dynamic, dynamic>()
        .when(any([typeIs<String>(), typeIs<num>(), typeIs<bool>()]), (v) => v)
        .whenIs<Iterable>(
            (iterable) => iterable.map((e) => _fromJson(e, dynamic)).toList())
        .whenIs<Map<String, dynamic>>((map) => map.map(
            (String key, value) => MapEntry(key, _fromJson(value, dynamic))))
        .apply(json);
  }

  @override
  DeleteStep<T> deleteFrom<T>([Type? type]) => _DeleteStep<T>(this, type ?? T);

  void _createIndexes() {
    registry.entries.forEach((type, entitySupport) {
      var sequence = 1;
      entitySupport.indexes.forEach((index) {
        var tableName = _snakeCase(entitySupport.name);
        var keys = <String, dynamic>{};
        index.fields.forEach((field) {
          keys[field.name] =
              field.direction == Direction.ascending ? 'asc' : 'desc';
        });
        _connection.execute(
            'create index if not exists ${tableName}_idx_$sequence'
            ' on $tableName (${keys.entries.map((entry) => '${entry.key} ${entry.value}').join(', ')}');
      });
    });
  }
}

class _DeleteStep<T> extends _TypedStep<T, _DeleteStep<T>>
    implements DeleteStep<T> {
  @override
  final PostgresBox box;
  @override
  final Type type;
  @override
  final String condition;
  @override
  final Map<String, dynamic> bindings;
  @override
  final Map<String, int> latestIndex;
  @override
  final Map<_Table, String> joins;

  _DeleteStep(this.box, this.type)
      : condition = '',
        bindings = {},
        latestIndex = {},
        joins = {};

  @override
  _DeleteStep<T> addCondition(
          String condition, Map<String, dynamic> bindings) =>
      _DeleteStep.withCondition(this, condition, bindings);

  _DeleteStep.withCondition(
      _DeleteStep<T> step, String condition, Map<String, dynamic> bindings)
      : box = step.box,
        type = step.type,
        latestIndex = step.latestIndex,
        joins = step.joins,
        condition = condition,
        bindings = bindings;

  @override
  Future execute() async {
    var connection = await box._openConnection;
    var entitySupport = box.registry.lookup(type);
    var tableName = _snakeCase(entitySupport.name);
    await connection.execute(
      'DELETE FROM "$tableName"${condition.isNotEmpty ? ' WHERE $condition' : ''}',
      substitutionValues: bindings,
    );
  }

  @override
  WhereStep<T, DeleteStep<T>> where(String field) =>
      _DeleteWhereStep(field, this);
}

class _DeleteWhereStep<T> extends _WhereStep<T, _DeleteStep<T>> {
  _DeleteWhereStep(String field, _DeleteStep<T> delete) : super(field, delete);

  @override
  _DeleteStep<T> createNextStep(
          String condition, Map<String, dynamic> bindings) =>
      _DeleteStep<T>.withCondition(step, combine(condition), bindings);
}

abstract class _TypedStep<T, S extends _TypedStep<T, S>> {
  Type get type;

  PostgresBox get box;

  String get condition;

  Map<String, dynamic> get bindings;

  Map<String, int> get latestIndex;

  Map<_Table, String> get joins;

  WhereStep<T, S> and(String field) => _AndStep(field, this as S);

  WhereStep<T, S> or(String field) => _OrStep(field, this as S);

  S addCondition(String condition, Map<String, dynamic> bindings);

  String index(String field) {
    var latest = latestIndex[field] ?? 0;
    var result = '$field${++latest}';
    latestIndex[field] = latest;
    return result.replaceAll('.', '_');
  }

  Map<String, dynamic> indexIterable(String field, Iterable<dynamic> values) =>
      {for (var v in values) index(field): v};
}

class _SelectStep implements SelectStep {
  final PostgresBox _box;
  final List<Field> _fields;

  _SelectStep(this._box, this._fields);

  @override
  _QueryStep from(Type type, [String? alias]) => _QueryStep(_box, type, _fields);
}

class _QueryStep<T> extends _ExpectationStep<T>
    with _TypedStep<T, _QueryStep<T>>
    implements QueryStep<T> {
  @override
  final Map<String, int> latestIndex;

  _QueryStep(PostgresBox box, Type? type, List<Field> fields)
      : latestIndex = {},
        super(box, type ?? T, fields);

  _QueryStep.withCondition(
      _QueryStep<T> query, String condition, Map<String, dynamic> bindings)
      : latestIndex = query.latestIndex,
        super.fromExisting(query, conditions: condition, bindings: bindings);

  _QueryStep.withJoin(_QueryStep<T> query, Type type, String join,
      Map<String, dynamic> bindings)
      : latestIndex = query.latestIndex,
        super.fromExisting(query, bindings: bindings, joins: {
          ...query.joins,
          _Table(type, _snakeCase(query.box.registry.lookup(type).name)): join
        });

  @override
  OrderByStep<T> orderBy(String field) => _OrderByStep(field, this);

  @override
  WhereStep<T, QueryStep<T>> where(String field) =>
      _QueryWhereStep(field, this);

  @override
  JoinStep<T> innerJoin(Type type, [String? alias]) => _JoinStep(type, this);

  @override
  _QueryStep<T> addCondition(String condition, Map<String, dynamic> bindings) =>
      _QueryStep.withCondition(this, condition, bindings);
}

class _JoinStep<T> implements JoinStep<T> {
  final Type type;
  final _QueryStep<T> query;

  _JoinStep(this.type, this.query);

  @override
  WhereStep<T, QueryStep<T>> on(String field) =>
      _JoinOnStep(field, type, query);
}

class _JoinOnStep<T> extends _QueryWhereStep<T> {
  final Type type;

  _JoinOnStep(String field, this.type, _QueryStep<T> query)
      : super(field, query);

  @override
  _QueryStep<T> createNextStep(
          String condition, Map<String, dynamic> bindings) =>
      _QueryStep<T>.withJoin(step, type, combine(condition),
          Map.from(bindings)..addAll(step.bindings));

  @override
  _QueryStep<T> equals(dynamic value) {
    return createNextStep(
        '${_fieldName(field, joinType: type)} = ${_fieldName(value, joinType: type)}',
        {});
  }
}

class _WhereStep<T, S extends _TypedStep<T, S>> implements WhereStep<T, S> {
  final String field;
  final S step;

  _WhereStep(this.field, this.step);

  String combine(String condition) => condition;

  S createNextStep(String condition, Map<String, dynamic> bindings) =>
      step.addCondition(combine(condition), {...step.bindings, ...bindings});

  @override
  WhereStep<T, S> not() => _NotStep(this);

  @override
  S equals(dynamic value) {
    var index = step.index(field);
    return createNextStep('${_fieldName(field)} = @$index', {index: value});
  }

  @override
  S like(String expression) {
    var index = step.index(field);
    return createNextStep(
        '${_fieldName(field)} LIKE @$index', {index: expression});
  }

  @override
  S gt(dynamic value) {
    var index = step.index(field);
    return createNextStep('${_fieldName(field)} > @$index', {index: value});
  }

  @override
  S gte(dynamic value) {
    var index = step.index(field);
    return createNextStep('${_fieldName(field)} >= @$index', {index: value});
  }

  @override
  S lt(dynamic value) {
    var index = step.index(field);
    return createNextStep('${_fieldName(field)} < @$index', {index: value});
  }

  @override
  S lte(dynamic value) {
    var index = step.index(field);
    return createNextStep('${_fieldName(field)} <= @$index', {index: value});
  }

  @override
  S between(dynamic value1, dynamic value2) {
    var index1 = step.index(field);
    var index2 = step.index(field);
    return createNextStep('${_fieldName(field)} BETWEEN @$index1 AND @$index2',
        {index1: value1, index2: value2});
  }

  @override
  S in_(Iterable<dynamic> values) {
    var indexed = step.indexIterable(field, values);
    return createNextStep(
        '${_fieldName(field)} IN (${indexed.keys.map((f) => '@$f').join(', ')})',
        indexed);
  }

  @override
  S contains(dynamic value) {
    var index = step.index(field);
    if (step.box.objectRepresentation == ObjectRepresentation.json) {
      return createNextStep(
          '${_fieldName(field, asJson: true)} ? @$index', {index: value});
    } else {
      return createNextStep(
          '${_fieldName(field)} @> ARRAY[@$index]', {index: value});
    }
  }

  String _fieldName(String field, {bool asJson = false, Type? joinType}) {
    if (field.contains('.')) {
      var parts = field.split('.');
      if (step.box.objectRepresentation == ObjectRepresentation.json &&
          !_isTable(parts[0], joinType)) {
        var partsInBetween = parts.sublist(1, parts.length - 1);
        return '(${_snakeCase(parts.first)}'
            '${partsInBetween.isNotEmpty ? '->' + partsInBetween.map((part) => "'$part'").join('->') : ''}'
            "${asJson ? '->' : '->>'}'${parts.last}'${asJson ? ')::jsonb' : ')'}";
      } else {
        return '"${parts.map(_snakeCase).join('"."')}"';
      }
    } else {
      return _snakeCase(field);
    }
  }

  bool _isTable(String name, Type? joinType) =>
      step.box.registry.lookup(step.type).name == name ||
      (joinType != null && step.box.registry.lookup(joinType).name == name) ||
      step.joins.keys.any((joinTable) => joinTable.alias != null
          ? joinTable.alias == name
          : step.box.registry.lookup(joinTable.type).name == name);
}

class _QueryWhereStep<T> extends _WhereStep<T, _QueryStep<T>> {
  _QueryWhereStep(String field, _QueryStep<T> query) : super(field, query);

  @override
  _QueryStep<T> createNextStep(
          String condition, Map<String, dynamic> bindings) =>
      _QueryStep<T>.withCondition(step, condition, bindings);
}

class _AndStep<T, S extends _TypedStep<T, S>> extends _WhereStep<T, S> {
  _AndStep(String field, S step) : super(field, step);

  @override
  String combine(String conditions) => step.condition != null
      ? '(${step.condition} AND $conditions)'
      : conditions;
}

class _OrStep<T, S extends _TypedStep<T, S>> extends _WhereStep<T, S> {
  _OrStep(String field, S step) : super(field, step);

  @override
  String combine(String conditions) => step.condition != null
      ? '(${step.condition} OR $conditions)'
      : conditions;
}

class _NotStep<T, S extends _TypedStep<T, S>> extends _WhereStep<T, S> {
  _NotStep(_WhereStep<T, S> whereStep) : super(whereStep.field, whereStep.step);

  @override
  String combine(String conditions) => 'NOT $conditions';
}

class _OrderByStep<T> implements OrderByStep<T> {
  final String field;
  final _QueryStep<T> _query;

  _OrderByStep(this.field, this._query);

  @override
  ExpectationStep<T> ascending() => _ExpectationStep.fromExisting(_query,
      order: {..._query._order, field: 'ASC'});

  @override
  ExpectationStep<T> descending() => _ExpectationStep.fromExisting(_query,
      order: {..._query._order, field: 'DESC'});
}

class _ExpectationStep<T> extends ExpectationStep<T> {
  @override
  final PostgresBox box;
  final String condition;
  final Map<_Table, String> joins;
  final Map<String, dynamic> bindings;
  final Map<String, String> _order;
  final Type type;
  final List<Field> _selectFields;

  _ExpectationStep(this.box, this.type, this._selectFields)
      : condition = '',
        joins = {},
        bindings = {},
        _order = {};

  _ExpectationStep.fromExisting(
    _ExpectationStep step, {
    String? conditions,
    Map<String, dynamic>? bindings,
    Map<String, String>? order,
    Map<_Table, String>? joins,
    List<Field>? selectFields,
  })  : box = step.box,
        condition = conditions ?? step.condition,
        joins = joins ?? step.joins,
        bindings = bindings ?? step.bindings,
        _order = order ?? step._order,
        type = step.type,
        _selectFields = selectFields ?? step._selectFields;

  @override
  Stream<T> stream({int limit = 1000000, int offset = 0}) => box._query<T>(
      condition, bindings, _order, type, limit, offset, _selectFields, joins);
}

class _Table {
  final Type type;
  final String name;
  final String? alias;

  _Table(this.type, this.name, [this.alias]);
}

String _snakeCase(String field) => ReCase(field).snakeCase;

String _camelCase(String field) => ReCase(field).camelCase;
