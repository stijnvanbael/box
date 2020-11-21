library box.memory;

import 'package:box/box.dart';

class MemoryBox extends Box {
  final Map<String, Map> entities = {};

  MemoryBox(Registry registry) : super(registry);

  @override
  bool get persistent => false;

  @override
  Future store(Object entity) {
    return entitiesFor(entity.runtimeType).then((entities) {
      entities[keyOf(entity)] = entity;
    });
  }

  @override
  Future<T> find<T>(key, [Type type]) async {
    return entitiesFor(type ?? T).then((entitiesForType) {
      return entitiesForType != null ? entitiesForType[key is Map ? Composite(key) : key] : null;
    });
  }

  Stream<T> _query<T>(Type type, Predicate predicate, _Ordering ordering) {
    return Stream.fromFuture(entitiesFor(type).then((entities) {
      var list = List<T>.from(entities.values.where((item) => predicate != null ? predicate.evaluate(item) : true));
      if (ordering != null) list.sort((object1, object2) => ordering.compare(object1, object2));
      return list;
    })).expand((list) => list);
  }

  @override
  SelectStep select(List<Field> fields) => _SelectStep(this, fields);

  @override
  _QueryStep<T> selectFrom<T>([Type type, String alias]) {
    return _QueryStep<T>(this, type);
  }

  Future<Map> entitiesFor(Type type) {
    var entitySupport = registry.lookup(type);
    entities.putIfAbsent(entitySupport.name, () => {});
    return Future.value(entities[entitySupport.name]);
  }

  @override
  Future deleteAll<T>([Type type]) async {
    return (await entitiesFor(T ?? type)).clear();
  }

  @override
  Future close() async {}

  @override
  DeleteStep<T> deleteFrom<T>([Type type]) {
    throw UnimplementedError();
  }
}

class _SelectStep implements SelectStep {
  final Box _box;
  final List<Field> _fields;

  _SelectStep(this._box, this._fields);

  @override
  _QueryStep from(Type type, [String alias]) => _QueryStep(_box, type, _fields);
}

class _QueryStep<T> extends _ExpectationStep<T> implements QueryStep<T> {
  _QueryStep(Box box, [Type type, List<Field> selectFields]) : super(box, type ?? T, selectFields);

  _QueryStep.withPredicate(_QueryStep<T> query, Predicate<T> predicate, Type type)
      : super(query.box, type, query.selectFields, predicate);

  @override
  WhereStep<T,QueryStep<T>> where(String field) => _QueryWhereStep(field, this);

  @override
  OrderByStep<T> orderBy(String field) => _OrderByStep(field, this);

  @override
  WhereStep<T,QueryStep<T>> and(String field) => _AndStep(field, this);

  @override
  WhereStep<T,QueryStep<T>> or(String field) => _OrStep(field, this);

  @override
  JoinStep<T> innerJoin(Type type, [String alias]) {
    // TODO: implement innerJoin
    throw UnimplementedError();
  }
}

class _OrStep<T> extends _QueryWhereStep<T> {
  _OrStep(String field, _QueryStep<T> query) : super(field, query);

  @override
  Predicate<T> combine(Predicate<T> predicate) => query.predicate != null ? query.predicate.or(predicate) : predicate;
}

class _AndStep<T> extends _QueryWhereStep<T> {
  _AndStep(String field, _QueryStep<T> query) : super(field, query);

  @override
  Predicate<T> combine(Predicate<T> predicate) => query.predicate != null ? query.predicate.and(predicate) : predicate;
}

class _ExpectationStep<T> extends ExpectationStep<T> {
  @override
  final MemoryBox box;
  final Predicate<T> predicate;
  final _Ordering<T> ordering;
  final Type _type;
  final List<Field> selectFields;

  _ExpectationStep(this.box, [this._type, this.selectFields, this.predicate, this.ordering]);

  @override
  Stream<T> stream({int limit = 1000000, int offset = 0}) {
    return box._query(_type, predicate, ordering).skip(offset).take(limit).map(_selectFields);
  }

  @override
  Future<T> unique() {
    return stream().first;
  }

  Type get type => T == dynamic ? _type : T;

  T _selectFields(dynamic record) {
    if (selectFields == null) {
      return record;
    }
    return {for (var field in selectFields) field.alias: box.registry.getFieldValue(field.name, record)} as T;
  }
}

class _QueryWhereStep<T> implements WhereStep<T, QueryStep<T>> {
  final String field;
  final _QueryStep<T> query;

  _QueryWhereStep(this.field, this.query);

  Predicate<T> combine(Predicate<T> predicate) => predicate;

  @override
  WhereStep<T, QueryStep<T>> not() => _NotStep<T>(this);

  @override
  QueryStep<T> like(String expression) => _queryStep(_LikePredicate(field, expression, query.box.registry));

  @override
  QueryStep<T> equals(dynamic value) => _queryStep(_EqualsPredicate(field, value, query.box.registry));

  @override
  QueryStep<T> gt(dynamic value) => _queryStep(_GreaterThanPredicate(field, value, query.box.registry));

  @override
  QueryStep<T> gte(dynamic value) => _queryStep(_GreaterThanOrEqualsPredicate(field, value, query.box.registry));

  @override
  QueryStep<T> lt(dynamic value) => _queryStep(_LessThanPredicate(field, value, query.box.registry));

  @override
  QueryStep<T> lte(dynamic value) => _queryStep(_LessThanOrEqualsPredicate(field, value, query.box.registry));

  @override
  QueryStep<T> between(dynamic value1, dynamic value2) =>
      _queryStep(_BetweenPredicate(field, value1, value2, query.box.registry));

  @override
  QueryStep<T> in_(Iterable<dynamic> values) => _queryStep(_InPredicate(field, values, query.box.registry));

  @override
  QueryStep<T> contains(dynamic value) => _queryStep(_ContainsPredicate(field, value, query.box.registry));

  QueryStep<T> _queryStep(Predicate<T> predicate) => _QueryStep.withPredicate(query, combine(predicate), query._type);
}

class _NotStep<T> extends _QueryWhereStep<T> {
  _NotStep(_QueryWhereStep<T> whereStep) : super(whereStep.field, whereStep.query);

  @override
  Predicate<T> combine(Predicate<T> predicate) => predicate.not();
}

class _OrderByStep<T> implements OrderByStep<T> {
  _QueryStep<T> query;
  String field;

  _OrderByStep(this.field, this.query);

  @override
  ExpectationStep<T> ascending() => _ExpectationStep(
      query.box, query.type, query.selectFields, query.predicate, _Ascending(query.type, field, query.box.registry));

  @override
  ExpectationStep<T> descending() => _ExpectationStep(
      query.box, query.type, query.selectFields, query.predicate, _Descending(query.type, field, query.box.registry));
}

class _Ascending<T> extends _Ordering<T> {
  _Ascending(Type type, String field, Registry registry) : super(type, field, registry);

  @override
  int compare(T object1, T object2) {
    var value1 = valueOf(object1);
    var value2 = valueOf(object2);
    return value1.toString().compareTo(value2);
  }
}

class _Descending<T> extends _Ordering<T> {
  _Descending(Type type, String field, Registry registry) : super(type, field, registry);

  @override
  int compare(T object1, T object2) {
    var value1 = valueOf(object1);
    var value2 = valueOf(object2);
    return -value1.toString().compareTo(value2);
  }
}

abstract class _Ordering<T> {
  final Type type;
  final String field;
  final Registry registry;

  _Ordering(this.type, this.field, this.registry);

  int compare(T object1, T object2);

  dynamic valueOf(T object) => registry.getFieldValue(field, object);
}

class _LikePredicate<T> extends _ExpressionPredicate<T, RegExp> {
  _LikePredicate(String field, String expression, Registry registry)
      : super(field, RegExp(expression.replaceAll('%', '.*'), caseSensitive: false), registry);

  @override
  bool evaluate(T object) {
    var value = valueOf(object);
    return value != null && expression.hasMatch(value.toString());
  }
}

class _EqualsPredicate<T, E> extends _ExpressionPredicate<T, E> {
  _EqualsPredicate(String field, E expression, Registry registry) : super(field, expression, registry);

  @override
  bool evaluate(T object) {
    var value = valueOf(object);
    return value != null && expression == value;
  }
}

abstract class _ComparingPredicate<T> extends _ExpressionPredicate<T, dynamic> {
  _ComparingPredicate(String field, dynamic expression, Registry registry) : super(field, expression, registry);

  @override
  bool evaluate(T object) {
    var value = valueOf(object);
    if (value == null) {
      return false;
    } else if (value is Comparable) {
      return compare(value.compareTo(expression));
    } else {
      throw 'Cannot compare "$object" with "$expression". Unsupported type';
    }
  }

  bool compare(int value);
}

class _GreaterThanPredicate<T> extends _ComparingPredicate<T> {
  _GreaterThanPredicate(String field, dynamic expression, Registry registry) : super(field, expression, registry);

  @override
  bool compare(int value) => value > 0;
}

class _GreaterThanOrEqualsPredicate<T> extends _ComparingPredicate<T> {
  _GreaterThanOrEqualsPredicate(String field, dynamic expression, Registry registry)
      : super(field, expression, registry);

  @override
  bool compare(int value) => value >= 0;
}

class _LessThanPredicate<T> extends _ComparingPredicate<T> {
  _LessThanPredicate(String field, dynamic expression, Registry registry) : super(field, expression, registry);

  @override
  bool compare(int value) => value < 0;
}

class _LessThanOrEqualsPredicate<T> extends _ComparingPredicate<T> {
  _LessThanOrEqualsPredicate(String field, dynamic expression, Registry registry) : super(field, expression, registry);

  @override
  bool compare(int value) => value <= 0;
}

class _BetweenPredicate<T> extends Predicate<T> {
  final Predicate<T> _lowerBound;
  final Predicate<T> _upperBound;

  _BetweenPredicate(String field, dynamic value1, dynamic value2, Registry registry)
      : _lowerBound = _GreaterThanPredicate(field, value1, registry),
        _upperBound = _LessThanPredicate(field, value2, registry);

  @override
  bool evaluate(T object) => _lowerBound.evaluate(object) && _upperBound.evaluate(object);
}

class _InPredicate<T, E> extends _ExpressionPredicate<T, List<E>> {
  _InPredicate(String field, List<E> values, Registry registry) : super(field, values, registry);

  @override
  bool evaluate(T object) {
    var value = valueOf(object);
    return value != null && expression.contains(value.toString());
  }
}

class _ContainsPredicate<T, E> extends _ExpressionPredicate<T, E> {
  _ContainsPredicate(String field, E value, Registry registry) : super(field, value, registry);

  @override
  bool evaluate(T object) {
    var value = valueOf(object);
    return value != null && value.contains(expression);
  }
}

abstract class _ExpressionPredicate<T, E> extends Predicate<T> {
  final String field;
  final E expression;
  final Registry registry;

  _ExpressionPredicate(this.field, this.expression, this.registry);

  dynamic valueOf(T object) => registry.getFieldValue(field, object);
}
