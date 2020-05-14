library box.mongodb;

import 'package:box/core.dart';
import 'package:inflection2/inflection2.dart';
import 'package:meta/meta.dart';
import 'package:mongo_dart/mongo_dart.dart' show Db, where, DbCollection, State, ObjectId;

class MongoDbBox extends Box {
  final Db _db;

  @override
  bool get persistent => true;

  MongoDbBox(
    String hostname,
    Registry registry, {
    int port = 27017,
    @required String database,
  })  : _db = Db('mongodb://$hostname:$port/$database'),
        super(registry);

  @override
  Future<T> find<T>(key, [Type type]) async {
    var collection = await _collectionFor<T>(type);
    var document = await collection.findOne(where.eq('_id', _toId(key)));
    return _toEntity<T>(document, type);
  }

  @override
  QueryStep<T> selectFrom<T>([Type type]) => _QueryStep<T>(this, type, null);

  @override
  Future store(dynamic entity) async {
    var document = _wrapKey(entity.toJson(), registry.lookup(entity.runtimeType).keyFields);
    var collection = await _collectionFor(entity.runtimeType);
    await collection.save(document);
  }

  Future<DbCollection> _collectionFor<T>(Type type) async {
    if (_db.state == State.INIT) {
      await _db.open();
    }
    return _db.collection(_collectionNameFor(T == dynamic ? type : T));
  }

  String _collectionNameFor(Type type) => convertToSpinalCase(registry.lookup(type).name);

  @override
  Future deleteAll<T>([Type type]) async {
    var collection = await _collectionFor<T>(type);
    await collection.drop();
  }

  dynamic _toEntity<T>(Map<String, dynamic> document, Type type) {
    var entitySupport = registry.lookup(T == dynamic ? type : T);
    return entitySupport.deserialize(_unwrapKey(document, entitySupport.keyFields));
  }

  @override
  Future close() async => _db.close();

  @override
  SelectStep select(List<Field> fields) => _SelectStep(this, fields);

  Map _unwrapKey(Map<String, dynamic> document, List<String> keyFields) {
    if (document == null) {
      return null;
    }
    var unwrapped = Map<String, dynamic>.from(document);
    var key = document['_id'];
    if (keyFields.length == 1) {
      unwrapped[keyFields.first] = key;
    } else {
      unwrapped.addAll(key);
    }
    unwrapped.remove('_id');
    return unwrapped;
  }

  Map _wrapKey(Map<String, dynamic> document, List<String> keyFields) {
    if (document == null) {
      return null;
    }
    var wrapped = Map<String, dynamic>.from(document);
    if (keyFields.length == 1) {
      wrapped['_id'] = document[keyFields.first];
    } else {
      wrapped['_id'] = {for (var key in keyFields) key: document[key]};
    }
    wrapped.removeWhere((key, value) => keyFields.contains(key));
    return wrapped;
  }
}

class _SelectStep implements SelectStep {
  final Box _box;
  final List<Field> _fields;

  _SelectStep(this._box, this._fields);

  @override
  _QueryStep from(Type type) => _QueryStep(_box, type, _fields);
}

class _QueryStep<T> extends _ExpectationStep<T> implements QueryStep<T> {
  _QueryStep(MongoDbBox box, Type type, List<Field> fields) : super(box, {}, {}, type ?? T, fields);

  _QueryStep.withSelector(_QueryStep<T> query, Map<String, dynamic> selector)
      : super(query.box, selector, query._order, query._type, query._selectFields);

  _QueryStep.withOrder(_QueryStep<T> query, Map<String, int> order)
      : super(query.box, query._selector, query._order..addAll(order), query._type, query._selectFields);

  @override
  OrderByStep<T> orderBy(String field) {
    return _OrderByStep(field, this);
  }

  @override
  WhereStep<T> where(String field) => _WhereStep(field, this);

  @override
  WhereStep<T> and(String field) => _AndStep(field, this);

  @override
  WhereStep<T> or(String field) => _OrStep(field, this);
}

class _OrStep<T> extends _WhereStep<T> {
  _OrStep(String field, _QueryStep<T> query) : super(field, query);

  @override
  Map<String, dynamic> combine(Map<String, dynamic> selector) => query._selector != null
      ? {
          r'$or': [query._selector, selector]
        }
      : selector;
}

class _AndStep<T> extends _WhereStep<T> {
  _AndStep(String field, _QueryStep<T> query) : super(field, query);

  @override
  Map<String, dynamic> combine(Map<String, dynamic> selector) => query._selector != null
      ? {
          r'$and': [query._selector, selector]
        }
      : selector;
}

class _OrderByStep<T> implements OrderByStep<T> {
  final String field;
  final _QueryStep<T> _query;

  _OrderByStep(this.field, this._query);

  @override
  ExpectationStep<T> ascending() =>
      _ExpectationStep(_query.box, _query._selector, {field: 1}, _query._type, _query._selectFields);

  @override
  ExpectationStep<T> descending() =>
      _ExpectationStep(_query.box, _query._selector, {field: -1}, _query._type, _query._selectFields);
}

class _ExpectationStep<T> extends ExpectationStep<T> {
  @override
  final MongoDbBox box;
  final Map<String, dynamic> _selector;
  final Map<String, int> _order;
  final Type _type;
  final List<Field> _selectFields;

  _ExpectationStep(this.box, this._selector, this._order, this._type, this._selectFields);

  @override
  Stream<T> stream({int limit = 1000000, int offset = 0}) async* {
    var collection = await box._collectionFor<T>(_type);
    yield* collection
        .find({r'$query': _selector, r'$orderby': _order})
        .skip(offset) // TODO: find a more efficient way to do this
        .take(limit)
        .map(_applySelectFields);
  }

  T _applySelectFields(Map<String, dynamic> record) {
    if (_selectFields == null) {
      return box._toEntity<T>(record, _type);
    }
    var map = {for (var field in _selectFields) field.alias: _getFieldValue(record, field.name)};
    return map as T;
  }

  dynamic _getFieldValue(Map<String, dynamic> record, String name) => name.contains('.')
      ? _getFieldValue(record[name.substring(0, name.indexOf('.'))], name.substring(name.indexOf('.') + 1))
      : record[name];

  @override
  Future<T> unique() async {
    var collection = await box._collectionFor<T>(_type);
    var document = await collection.findOne(_selector);
    return box._toEntity<T>(document, _type);
  }
}

class _WhereStep<T> implements WhereStep<T> {
  final String field;
  final _QueryStep<T> query;

  _WhereStep(String field, this.query) : field = _translate(field, query._type, query.box.registry);

  Map<String, dynamic> combine(Map<String, dynamic> selector) => selector;

  QueryStep<T> _queryStep(Map<String, dynamic> selector) =>
      _QueryStep<T>.withSelector(query, combine({field: selector}));

  @override
  WhereStep<T> not() => _NotStep<T>(this);

  @override
  QueryStep<T> equals(dynamic value) => _queryStep({r'$eq': value});

  @override
  QueryStep<T> like(String expression) => _queryStep({r'$regex': expression.replaceAll('%', '.*'), r'$options': 'i'});

  @override
  QueryStep<T> gt(dynamic value) => _queryStep({r'$gt': value});

  @override
  QueryStep<T> gte(dynamic value) => _queryStep({r'$gte': value});

  @override
  QueryStep<T> lt(dynamic value) => _queryStep({r'$lt': value});

  @override
  QueryStep<T> lte(dynamic value) => _queryStep({r'$lte': value});

  @override
  QueryStep<T> between(dynamic value1, dynamic value2) => _queryStep({r'$gt': value1, r'$lt': value2});

  static String _translate(String field, Type type, Registry registry) =>
      !field.contains('.') && registry.lookup(type).isKey(field) ? '_id' : field;
}

class _NotStep<T> extends _WhereStep<T> {
  _NotStep(_WhereStep<T> whereStep) : super(whereStep.field, whereStep.query);

  @override
  Map<String, dynamic> combine(Map<String, dynamic> selector) => selector.map((k, v) => MapEntry(k, {r'$not': v}));
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
    return Map<String, dynamic>.from(value.map((k, v) => MapEntry(k, _toId(v))));
  }
  if (value is List) {
    return List.from(value.map((e) => _toId(e)));
  }
  return value?.toString();
}
