import 'package:box/core.dart';
import 'package:box/src/core.dart';
import 'package:inflection2/inflection2.dart';
import 'package:meta/meta.dart';
import 'package:mongo_dart/mongo_dart.dart';
import 'package:reflective/reflective.dart';

class MongoDbBox extends Box {
  Db _db;

  MongoDbBox(
    String hostname, {
    int port: 27017,
    @required String database,
  }) : _db = Db('mongodb://$hostname:$port/$database') {
    Converters.add(_ObjectToDocument());
    Converters.add(_DocumentToObject());
  }

  @override
  Future<T> find<T>(key) async {
    var collection = await _collectionFor(T);
    var document = await collection.findOne(where.eq('_id', _toId(key)));
    return _toEntity<T>(document);
  }

  @override
  QueryStep<T> selectFrom<T>() => _QueryStep<T>(this);

  @override
  Future store(Object entity) async {
    var document = Conversion.convert(entity).to(Map);
    var collection = await _collectionFor(entity.runtimeType);
    collection.save(document);
  }

  Future<DbCollection> _collectionFor(Type type) async {
    if (_db.state == State.INIT) {
      await _db.open();
    }
    return _db.collection(_collectionNameFor(type));
  }

  String _collectionNameFor(Type type) => convertToSpinalCase(TypeReflection(type).name);

  @override
  Future deleteAll<T>() async {
    var collection = await _collectionFor(T);
    await collection.drop();
  }

  _toEntity<T>(Map<String, dynamic> document) {
    return Conversion.convert(document).to(T);
  }
}

class _QueryStep<T> extends _ExpectationStep<T> implements QueryStep<T> {
  _QueryStep(MongoDbBox box) : super(box, {}, {});

  _QueryStep.withSelector(_QueryStep<T> query, Map<String, dynamic> selector)
      : super(query._box, selector, query._order);

  _QueryStep.withOrder(_QueryStep<T> query, Map<String, int> order)
      : super(query._box, query._selector, query._order..addAll(order));

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
  ExpectationStep<T> ascending() => _ExpectationStep(_query._box, _query._selector, {field: 1});

  @override
  ExpectationStep<T> descending() => _ExpectationStep(_query._box, _query._selector, {field: -1});
}

class _ExpectationStep<T> extends ExpectationStep<T> {
  final MongoDbBox _box;
  final Map<String, dynamic> _selector;
  final Map<String, int> _order;

  _ExpectationStep(this._box, this._selector, this._order);

  @override
  Stream<T> stream() async* {
    var collection = await _box._collectionFor(T);
    yield* collection.find({r'$query': _selector, r'$orderby': _order}).map((document) => _box._toEntity<T>(document));
  }

  @override
  Future<T> unique() async {
    var collection = await _box._collectionFor(T);
    var document = await collection.findOne(_selector);
    return _box._toEntity<T>(document);
  }
}

class _WhereStep<T> implements WhereStep<T> {
  final String field;
  final _QueryStep<T> query;

  _WhereStep(this.field, this.query);

  @override
  QueryStep<T> equals(dynamic value) => _QueryStep<T>.withSelector(
      query,
      combine({
        field: {r'$eq': value}
      }));

  @override
  QueryStep<T> like(String expression) => _QueryStep<T>.withSelector(
      query,
      combine({
        field: {r'$regex': expression.replaceAll('%', '.+')}
      }));

  @override
  WhereStep<T> not() => _NotStep<T>(this);

  Map<String, dynamic> combine(Map<String, dynamic> selector) => selector;
}

class _NotStep<T> extends _WhereStep<T> {
  _NotStep(_WhereStep<T> whereStep) : super(whereStep.field, whereStep.query);

  @override
  Map<String, dynamic> combine(Map<String, dynamic> selector) => selector.map((k, v) => MapEntry(k, {r'$not': v}));
}

var objectIdPattern = new RegExp(r'^[0-9a-fA-F]+$');

class _ObjectToDocument extends ConverterBase<Object, Map<String, dynamic>> {
  _ObjectToDocument() : super(TypeReflection<Object>(), TypeReflection<Map<String, dynamic>>());

  Map<String, dynamic> convertTo(Object object, TypeReflection targetReflection) {
    return _convert(object);
  }

  dynamic _convert(object) {
    if (object == null || object is String || object is num || object is bool || object is DateTime) {
      return object;
    } else if (object is Iterable) {
      return List.from(object.map((item) => _convert(item)));
    } else if (object is Map) {
      Map map = <String, dynamic>{};
      object.keys.forEach((k) => map[k.toString()] = _convert(object[k]));
      return map;
    } else {
      TypeReflection type = TypeReflection.fromInstance(object);
      var ids = type.fields.values
          .where((field) => field.has(Key))
          .map((field) => {field.name: _toId(field.value(object))})
          .toList();
      var fields = type.fields.values.where((field) => !field.has(Transient) && !field.has(Key)).map((field) {
        return {field.name: _convert(field.value(object))};
      }).toList();
      if (ids.length == 1) {
        fields.add({'_id': ids[0].values.first});
      } else {
        fields.add({'_id': ids.reduce((Map m1, Map m2) => m2..addAll(m1))});
      }
      return fields.reduce((Map m1, Map m2) => m2..addAll(m1));
    }
  }
}

class _DocumentToObject extends ConverterBase<Map<String, dynamic>, Object> {
  _DocumentToObject() : super(TypeReflection<Map<String, dynamic>>(), TypeReflection<Object>());

  Object convertTo(Map<String, dynamic> document, TypeReflection targetReflection) {
    if (document != null && document.containsKey(r'$err')) {
      throw MongoDbError(document[r'$err']);
    }
    return _convert(document, targetReflection);
  }

  _convert(object, TypeReflection targetReflection) {
    if (object == null) {
      return null;
    } else if (object is Map) {
      if (targetReflection.sameOrSuper(Map)) {
        TypeReflection keyType = targetReflection.typeArguments[0];
        TypeReflection valueType = targetReflection.typeArguments[1];
        Map map = {};
        object.keys.forEach((k) {
          var newKey = keyType.sameOrSuper(k) ? k : keyType.construct(args: [k]);
          map[newKey] = _convert(object[k], valueType);
        });
        return map;
      } else {
        var instance = targetReflection.construct();
        object.keys.forEach((k) {
          if (k != '_id' && targetReflection.fields[k] == null)
            throw JsonException('Unknown property: ' + targetReflection.name + '.' + k);
        });
        Maps.forEach(targetReflection.fields, (name, field) {
          if (field.has(Key)) {
            field.set(instance, _parseId(object['_id']));
          } else {
            field.set(instance, _convert(object[name], field.type));
          }
        });
        return instance;
      }
    } else if (object is Iterable) {
      TypeReflection itemType = targetReflection.typeArguments[0];
      return List.from(object.map((i) => _convert(i, itemType)));
    } else {
      return object;
    }
  }
}

class MongoDbError extends Error {
  String message;

  MongoDbError(this.message);

  String toString() => message;
}

dynamic _toId(dynamic value) => value is String && objectIdPattern.hasMatch(value)
    ? ObjectId.fromHexString(value)
    : value is String || value is num || value is DateTime || value is bool ? value : Conversion.convert(value).to(Map);

dynamic _parseId(dynamic value) => value is ObjectId ? value.toHexString() : value;
