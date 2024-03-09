library box.mongodb;

import 'dart:io';

import 'package:box/core.dart';
import 'package:box/mongodb.dart';
import 'package:mongo_dart/mongo_dart.dart'
    show
        ConnectionException,
        Db,
        DbCollection,
        ObjectId,
        State,
        WriteConcern,
        where;
import 'package:recase/recase.dart';

class MongoDbBox extends Box {
  final String connectionString;
  Db? _db;

  @override
  bool get persistent => true;

  MongoDbBox(this.connectionString, Registry registry) : super(registry);

  @override
  Future<T?> find<T>(key, [Type? type]) => _autoRecover(() async {
        var collection = await _collectionFor<T>(type);
        var document = await collection.findOne(where.eq('_id', _toId(key)));
        return document != null ? _toEntity<T>(document, type) : null;
      });

  @override
  QueryStep<T> selectFrom<T>([Type? type, String? alias]) =>
      _QueryStep<T>(this, type, null);

  @override
  Future<K> store<K>(dynamic entity) => _autoRecover(() async {
        var entitySupport = registry.lookup(entity.runtimeType);
        var document =
            _wrapKey(entitySupport.serialize(entity), entitySupport.keyFields);
        var collection = await _collectionFor(entity.runtimeType);
        var result = await collection.replaceOne(
          {'_id': document['_id']},
          document,
          upsert: true,
        );
        if (result.isFailure) {
          throw StateError('Failed to upsert: ${result.writeError!.errmsg}');
        }
        var id = document['_id'];
        return (id is ObjectId ? id.toHexString() : id) as K;
      });

  @override
  Future insertAll<T>(Iterable<T> entities) => _autoRecover(() async {
        var entitySupport = registry.lookup<T>();
        var collection = await _collectionFor(T);
        var documents = entities
            .map((entity) => _wrapKey(
                entitySupport.serialize(entity), entitySupport.keyFields))
            .toList();
        await collection.insertAll(documents,
            writeConcern: WriteConcern.acknowledged);
      });

  Future<DbCollection> _collectionFor<T>(Type? type) async {
    await _connect();
    return _db!.collection(_collectionNameFor(type ?? T));
  }

  Future _connect() async {
    if (_db == null || _db!.state != State.open || !_db!.isConnected) {
      try {
        _db = await Db.create(connectionString);
        await _db!.open(secure: connectionString.startsWith('mongodb+srv:'));
        while (_db!.state == State.opening) {
          await Future.delayed(Duration(milliseconds: 10));
        }
        if (_db!.state != State.open) {
          throw StateError('Failed to open connection');
        }
        _createIndexes();
      } catch (e) {
        _db = null;
        rethrow;
      }
    }
  }

  void _createIndexes() {
    registry.entries.forEach((type, entitySupport) {
      entitySupport.indexes.forEach((index) {
        var collectionName = _collectionNameFor(type);
        var keys = <String, dynamic>{};
        index.fields.forEach((field) {
          keys[field.name] = field.direction == Direction.ascending ? 1 : -1;
        });
        _db!.createIndex(collectionName, keys: keys);
      });
    });
  }

  String _collectionNameFor(Type type) =>
      ReCase(registry.lookup(type).name).paramCase;

  @override
  Future deleteAll<T>([Type? type]) => _autoRecover(() async {
        var collection = await _collectionFor<T>(type);
        await collection.drop();
      });

  dynamic _toEntity<T>(Map<String, dynamic>? document, Type? type) {
    if (document == null) {
      return null;
    }
    var entitySupport = registry.lookup(type ?? T);
    var unwrapped = _unwrapKey(document, entitySupport.keyFields);
    return entitySupport.deserialize(unwrapped);
  }

  @override
  Future close() async => _db!.close();

  @override
  SelectStep select(List<Field> fields) => _SelectStep(this, fields);

  Map<String, dynamic> _unwrapKey(
    Map<String, dynamic> document,
    List<String> keyFields,
  ) {
    var unwrapped = Map<String, dynamic>.from(document);
    var key = document['_id'];
    if (keyFields.length == 1) {
      unwrapped[keyFields.first] = key is ObjectId ? key.toHexString() : key;
    } else {
      unwrapped.addAll(key);
    }
    unwrapped.remove('_id');
    return unwrapped;
  }

  Map<String, dynamic> _wrapKey(
      Map<String, dynamic> document, List<String> keyFields) {
    var wrapped = Map<String, dynamic>.from(document);
    if (keyFields.length == 1) {
      var key = document[keyFields.first];
      if (key?.length == 24) {
        try {
          key = ObjectId.fromHexString(key);
        } catch (e) {
          // Not a hex string
        }
      }
      key ??= ObjectId();
      wrapped['_id'] = key;
    } else {
      wrapped['_id'] = {for (var key in keyFields) key: document[key]};
    }
    wrapped.removeWhere((key, value) => keyFields.contains(key));
    return wrapped;
  }

  @override
  DeleteStep<T> deleteFrom<T>([Type? type]) => _DeleteStep<T>(this, type ?? T);

  @override
  UpdateStep<T> update<T>([Type? type]) => _UpdateStep<T>(this, type ?? T);
}

class _UpdateStep<T> extends _TypedStep<T, _UpdateStep<T>>
    implements UpdateStep<T>, UpdateWhereStep<T> {
  @override
  final MongoDbBox box;
  @override
  final Type type;
  @override
  final Map<String, dynamic> selector;

  Map<String, dynamic> setUpdates = {};

  _UpdateStep(this.box, this.type, [this.selector = const {}]);

  _UpdateStep.withSelector(
    _UpdateStep<T> step,
    Map<String, dynamic> selector,
  )   : box = step.box,
        type = step.type,
        selector = selector,
        setUpdates = step.setUpdates;

  @override
  Future<int> execute() => _autoRecover(() async {
        var collection = await box._collectionFor(type);
        return (await collection.updateMany(
          selector,
          {'\$set': setUpdates},
          writeConcern: WriteConcern.acknowledged,
        ))
            .nModified;
      });

  @override
  UpdateStep<T> set(String field, dynamic value) {
    setUpdates[field] = value;
    return this;
  }

  @override
  WhereStep<T, UpdateWhereStep<T>> where(String field) =>
      _UpdateWhereStep(field, this);

  @override
  _UpdateStep<T> addSelector(Map<String, dynamic> selector) =>
      _UpdateStep.withSelector(this, selector);
}

class _DeleteStep<T> extends _TypedStep<T, _DeleteStep<T>>
    implements DeleteStep<T> {
  @override
  final MongoDbBox box;
  @override
  final Type type;
  @override
  final Map<String, dynamic>? selector;

  _DeleteStep(this.box, this.type) : selector = {};

  @override
  _DeleteStep<T> addSelector(Map<String, dynamic>? selector) =>
      _DeleteStep.withSelector(this, selector);

  _DeleteStep.withSelector(_DeleteStep<T> step, Map<String, dynamic>? selector)
      : box = step.box,
        type = step.type,
        selector = selector;

  @override
  Future<int> execute() => _autoRecover(() async {
        final collection = await box._collectionFor(type);
        final removed = await collection.remove(selector,
            writeConcern: WriteConcern.acknowledged);
        return removed['nRemoved'];
      });

  @override
  WhereStep<T, DeleteStep<T>> where(String field) =>
      _DeleteWhereStep(field, this);
}

class _SelectStep implements SelectStep {
  final MongoDbBox _box;
  final List<Field> _fields;

  _SelectStep(this._box, this._fields);

  @override
  _QueryStep from(Type type, [String? alias]) =>
      _QueryStep(_box, type, _fields);
}

class _QueryStep<T> extends _ExpectationStep<T>
    with _TypedStep<T, _QueryStep<T>>
    implements QueryStep<T> {
  _QueryStep(MongoDbBox box, Type? type, List<Field>? fields)
      : super(box, {}, {}, type ?? T, fields);

  _QueryStep.withSelector(_QueryStep<T> query, Map<String, dynamic> selector)
      : super(
            query.box, selector, query._order, query.type, query._selectFields);

  @override
  OrderByStep<T> orderBy(String field) => _OrderByStep(field, this);

  @override
  WhereStep<T, QueryStep<T>> where(String field) =>
      _QueryWhereStep(field, this);

  @override
  JoinStep<T> innerJoin(Type type, [String? alias]) {
    // TODO: implement innerJoin
    throw UnimplementedError();
  }

  @override
  _QueryStep<T> addSelector(Map<String, dynamic> selector) =>
      _QueryStep.withSelector(this, selector);
}

class _OrStep<T, S extends _TypedStep<T, S>> extends _WhereStep<T, S> {
  _OrStep(String field, S step) : super(field, step);

  @override
  Map<String, dynamic> combine(Map<String, dynamic> selector) =>
      step.selector != null
          ? {
              r'$or': [step.selector, selector]
            }
          : selector;
}

class _AndStep<T, S extends _TypedStep<T, S>> extends _WhereStep<T, S> {
  _AndStep(String field, S step) : super(field, step);

  @override
  Map<String, dynamic> combine(Map<String, dynamic> selector) =>
      step.selector != null
          ? {
              r'$and': [step.selector, selector]
            }
          : selector;
}

class _OrderByStep<T> implements OrderByStep<T> {
  final String field;
  final _QueryStep<T> _query;

  _OrderByStep(this.field, this._query);

  @override
  ExpectationStep<T> ascending() => _ExpectationStep<T>(_query.box,
      _query.selector, {field: 1}, _query.type, _query._selectFields);

  @override
  ExpectationStep<T> descending() => _ExpectationStep<T>(_query.box,
      _query.selector, {field: -1}, _query.type, _query._selectFields);
}

class _ExpectationStep<T> extends ExpectationStep<T> {
  @override
  final MongoDbBox box;
  final Map<String, dynamic>? selector;
  final Map<String, int> _order;
  final Type type;
  final List<Field>? _selectFields;

  _ExpectationStep(
      this.box, this.selector, this._order, this.type, this._selectFields);

  @override
  Stream<T> stream({int limit = 1000000, int offset = 0}) =>
      _autoRecoverStream(() async* {
        var collection = await box._collectionFor<T>(type);
        yield* collection
            .modernFind(
              filter: selector,
              sort: _order,
              skip: offset > 0 ? offset : null,
              limit: limit,
            )
            .map(_applySelectFields);
      });

  T _applySelectFields(Map<String, dynamic> record) {
    if (_selectFields == null) {
      return box._toEntity<T>(record, type);
    }
    var map = {
      for (var field in _selectFields!)
        field.alias: _getFieldValue(record, field.name)
    };
    return map as T;
  }

  dynamic _getFieldValue(Map<String, dynamic> record, String name) =>
      name.contains('.')
          ? _getFieldValue(record[name.substring(0, name.indexOf('.'))],
              name.substring(name.indexOf('.') + 1))
          : record[name];

  @override
  Future<T?> unique() => _autoRecover(() async {
        var collection = await box._collectionFor<T>(type);
        var document = await collection.findOne(selector);
        return box._toEntity<T>(document, type);
      });
}

abstract mixin class _TypedStep<T, S extends _TypedStep<T, S>> {
  Type get type;

  MongoDbBox get box;

  Map<String, dynamic>? get selector;

  WhereStep<T, S> and(String field) => _AndStep(field, this as S);

  WhereStep<T, S> or(String field) => _OrStep(field, this as S);

  S addSelector(Map<String, dynamic> selector);
}

abstract class _WhereStep<T, S extends _TypedStep<T, S>>
    implements WhereStep<T, S> {
  final String field;
  final S step;

  _WhereStep(String field, this.step)
      : field = _translate(field, step.type, step.box.registry);

  Map<String, dynamic> combine(Map<String, dynamic> selector) => selector;

  S createNextStep(Map<String, dynamic> selector) =>
      step.addSelector(combine({field: selector}));

  @override
  WhereStep<T, S> not() => _NotStep<T, S>(this);

  @override
  S equals(dynamic value) => createNextStep({r'$eq': _convert(value)});

  @override
  S like(String expression) => createNextStep(
      {r'$regex': expression.replaceAll('%', '.*'), r'$options': 'i'});

  @override
  S gt(dynamic value) => createNextStep({r'$gt': _convert(value)});

  @override
  S gte(dynamic value) => createNextStep({r'$gte': _convert(value)});

  @override
  S lt(dynamic value) => createNextStep({r'$lt': _convert(value)});

  @override
  S lte(dynamic value) => createNextStep({r'$lte': _convert(value)});

  @override
  S between(dynamic value1, dynamic value2) =>
      createNextStep({r'$gt': _convert(value1), r'$lt': _convert(value2)});

  static String _translate(String field, Type type, Registry registry) =>
      !field.contains('.') && registry.lookup(type).isKey(field)
          ? '_id'
          : field;

  @override
  S in_(Iterable<dynamic> values) =>
      createNextStep({r'$in': List.from(values)});

  @override
  S contains(dynamic value) => createNextStep({
        r'$all': [value]
      });

  dynamic _convert(dynamic value) {
    if (value == null) {
      return null;
    } else if (value is DateTime) {
      return value.toIso8601String();
    } else if (_isEnum(value)) {
      return value.toString().split('.')[1];
    }
    return value;
  }

  bool _isEnum(dynamic value) {
    var split = value.toString().split('.');
    return split.length > 1 && split[0] == value.runtimeType.toString();
  }
}

class _QueryWhereStep<T> extends _WhereStep<T, _QueryStep<T>> {
  _QueryWhereStep(String field, _QueryStep<T> query) : super(field, query);

  @override
  _QueryStep<T> createNextStep(Map<String, dynamic> selector) =>
      _QueryStep<T>.withSelector(step, combine({field: selector}));
}

class _DeleteWhereStep<T> extends _WhereStep<T, _DeleteStep<T>> {
  _DeleteWhereStep(String field, _DeleteStep<T> delete) : super(field, delete);

  @override
  _DeleteStep<T> createNextStep(Map<String, dynamic> selector) =>
      _DeleteStep<T>.withSelector(step, combine({field: selector}));
}

class _UpdateWhereStep<T> extends _WhereStep<T, _UpdateStep<T>> {
  _UpdateWhereStep(String field, _UpdateStep<T> update) : super(field, update);

  @override
  _UpdateStep<T> createNextStep(Map<String, dynamic> selector) =>
      _UpdateStep<T>.withSelector(step, combine({field: selector}));
}

class _NotStep<T, S extends _TypedStep<T, S>> extends _WhereStep<T, S> {
  _NotStep(_WhereStep<T, S> whereStep) : super(whereStep.field, whereStep.step);

  @override
  Map<String, dynamic> combine(Map<String, dynamic> selector) =>
      selector.map((k, v) => MapEntry(k, {r'$not': v}));
}

var objectIdPattern = RegExp(r'^[0-9a-fA-F]+$');

class MongoDbError extends Error {
  String message;

  MongoDbError(this.message);

  @override
  String toString() => message;
}

dynamic _toId(dynamic value) {
  if (value is String && objectIdPattern.hasMatch(value)) {
    return ObjectId.fromHexString(value);
  }
  if (value is String || value is num || value is bool) {
    return value;
  }
  if (value is DateTime) {
    return value.toIso8601String();
  }
  if (value is Map) {
    return Map<String, dynamic>.from(
        value.map((k, v) => MapEntry(k, _toId(v))));
  }
  if (value is List) {
    return List.from(value.map((e) => _toId(e)));
  }
  return value?.toString();
}

Future<R> _autoRecover<R>(Future<R> Function() action) async {
  try {
    sleep(Duration(milliseconds: 100));
    return await action();
  } on ConnectionException catch (e) {
    if (e.message.startsWith('connection closed')) {
      // connections tend to reset when connecting to Atlas, retry
      return await _autoRecover(action);
    } else {
      rethrow;
    }
  }
}

Stream<R> _autoRecoverStream<R>(Stream<R> Function() action) async* {
  try {
    sleep(Duration(milliseconds: 100));
    var results = await action().toList();
    yield* Stream.fromIterable(results);
  } on ConnectionException catch (e) {
    if (e.message.startsWith('connection closed')) {
      // connections tend to reset when connecting to Atlas, retry
      yield* _autoRecoverStream(action);
    } else {
      rethrow;
    }
  }
}
