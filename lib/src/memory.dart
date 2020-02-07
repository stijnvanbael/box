import 'package:box/box.dart';
import 'package:reflective/reflective.dart';

class MemoryBox extends Box {
  final Map<String, Map> entities = {};

  @override
  bool get persistent => false;

  @override
  Future store(Object entity) {
    TypeReflection type = TypeReflection.fromInstance(entity);
    return entitiesFor(type).then((entities) {
      entities[Box.keyOf(entity)] = entity;
    });
  }

  @override
  Future<T> find<T>(key) async {
    return entitiesFor(TypeReflection<T>()).then((entitiesForType) {
      return entitiesForType != null ? entitiesForType[key is Map ? Composite(key) : key] : null;
    });
  }

  Stream<T> _query<T>(TypeReflection<T> type, Predicate predicate, _Ordering ordering) {
    return Stream.fromFuture(entitiesFor(type).then((entities) {
      List<T> list = List.from(entities.values.where((item) => predicate != null ? predicate.evaluate(item) : true));
      if (ordering != null) list.sort((object1, object2) => ordering.compare(object1, object2));
      return list;
    })).expand((list) => list);
  }

  @override
  _QueryStep<T> selectFrom<T>() {
    return _QueryStep<T>(this);
  }

  Future<Map> entitiesFor(TypeReflection type) {
    entities.putIfAbsent(type.name, () => Map());
    return Future.value(entities[type.name]);
  }

  @override
  Future deleteAll<T>() async {
    return (await entitiesFor(TypeReflection<T>())).clear();
  }

  @override
  Future close() async {}
}

class _QueryStep<T> extends _ExpectationStep<T> implements QueryStep<T> {
  _QueryStep(Box box) : super(box);

  _QueryStep.withPredicate(_QueryStep<T> query, Predicate<T> predicate) : super(query.box, predicate);

  Type get type => T;

  @override
  WhereStep<T> where(String field) => _WhereStep(field, this);

  @override
  OrderByStep<T> orderBy(String field) => _OrderByStep(field, this);

  @override
  WhereStep<T> and(String field) => _AndStep(field, this);

  @override
  WhereStep<T> or(String field) => _OrStep(field, this);
}

class _OrStep<T> extends _WhereStep<T> {
  _OrStep(String field, _QueryStep<T> query) : super(field, query);

  @override
  Predicate<T> combine(Predicate<T> predicate) => query.predicate != null ? query.predicate.or(predicate) : predicate;
}

class _AndStep<T> extends _WhereStep<T> {
  _AndStep(String field, _QueryStep<T> query) : super(field, query);

  @override
  Predicate<T> combine(Predicate<T> predicate) => query.predicate != null ? query.predicate.and(predicate) : predicate;
}

class _ExpectationStep<T> extends ExpectationStep<T> {
  final MemoryBox box;
  final Predicate<T> predicate;
  final _Ordering<T> ordering;

  _ExpectationStep(this.box, [this.predicate, this.ordering]);

  @override
  Stream<T> stream({int limit = 1000000, int offset = 0}) {
    return box._query(TypeReflection<T>(), predicate, ordering).skip(offset).take(limit);
  }

  @override
  Future<T> unique() {
    return stream().first;
  }
}

class _WhereStep<T> implements WhereStep<T> {
  final String field;
  final _QueryStep<T> query;

  _WhereStep(this.field, this.query);

  @override
  WhereStep<T> not() => _NotStep<T>(this);

  @override
  QueryStep<T> like(String expression) =>
      _QueryStep.withPredicate(query, combine(_LikePredicate(query.type, field, expression)));

  @override
  QueryStep<T> equals(dynamic value) =>
      _QueryStep.withPredicate(query, combine(_EqualsPredicate(query.type, field, value)));

  Predicate<T> combine(Predicate<T> predicate) => predicate;

  @override
  QueryStep<T> gt(dynamic value) =>
      _QueryStep.withPredicate(query, combine(_GreaterThanPredicate(query.type, field, value)));

  @override
  QueryStep<T> gte(dynamic value) =>
      _QueryStep.withPredicate(query, combine(_GreaterThanOrEqualsPredicate(query.type, field, value)));

  @override
  QueryStep<T> lt(dynamic value) =>
      _QueryStep.withPredicate(query, combine(_LessThanPredicate(query.type, field, value)));

  @override
  QueryStep<T> lte(dynamic value) =>
      _QueryStep.withPredicate(query, combine(_LessThanOrEqualsPredicate(query.type, field, value)));

  @override
  QueryStep<T> between(dynamic value1, dynamic value2) =>
      _QueryStep.withPredicate(query, combine(_BetweenPredicate(query.type, field, value1, value2)));
}

class _NotStep<T> extends _WhereStep<T> {
  _NotStep(_WhereStep<T> whereStep) : super(whereStep.field, whereStep.query);

  @override
  Predicate<T> combine(Predicate<T> predicate) => predicate.not();
}

class _OrderByStep<T> implements OrderByStep<T> {
  _QueryStep<T> query;
  String field;

  _OrderByStep(this.field, this.query);

  @override
  ExpectationStep<T> ascending() => _ExpectationStep(query.box, query.predicate, _Ascending(query.type, field));

  @override
  ExpectationStep<T> descending() => _ExpectationStep(query.box, query.predicate, _Descending(query.type, field));
}

class _Ascending<T> extends _Ordering<T> {
  _Ascending(Type type, String field) : super(type, field);

  @override
  int compare(T object1, T object2) {
    var value1 = valueOf(object1);
    var value2 = valueOf(object2);
    return value1.toString().compareTo(value2);
  }
}

class _Descending<T> extends _Ordering<T> {
  _Descending(Type type, String field) : super(type, field);

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
  FieldReflection fieldReflection;

  _Ordering(this.type, this.field) {
    TypeReflection typeReflection = TypeReflection(type);
    this.fieldReflection = typeReflection.field(field);
    if (fieldReflection == null) {
      throw Exception('Field not found: $typeReflection.' + field);
    }
  }

  int compare(T object1, T object2);

  valueOf(T object) {
    return fieldReflection.value(object);
  }
}

class _LikePredicate<T> extends _ExpressionPredicate<T, RegExp> {
  _LikePredicate(Type type, String field, String expression)
      : super(type, field, RegExp(expression.replaceAll('%', '.*'), caseSensitive: false));

  @override
  bool evaluate(T object) {
    var value = valueOf(object);
    return value != null && expression.hasMatch(value.toString());
  }
}

class _EqualsPredicate<T> extends _ExpressionPredicate<T, String> {
  _EqualsPredicate(Type type, String field, String expression) : super(type, field, expression);

  @override
  bool evaluate(T object) {
    var value = valueOf(object);
    return value != null && expression == value.toString();
  }
}

abstract class _ComparingPredicate<T> extends _ExpressionPredicate<T, dynamic> {
  _ComparingPredicate(Type type, String field, dynamic expression) : super(type, field, expression);

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
  _GreaterThanPredicate(Type type, String field, dynamic expression) : super(type, field, expression);

  @override
  bool compare(int value) => value > 0;
}

class _GreaterThanOrEqualsPredicate<T> extends _ComparingPredicate<T> {
  _GreaterThanOrEqualsPredicate(Type type, String field, dynamic expression) : super(type, field, expression);

  @override
  bool compare(int value) => value >= 0;
}

class _LessThanPredicate<T> extends _ComparingPredicate<T> {
  _LessThanPredicate(Type type, String field, dynamic expression) : super(type, field, expression);

  @override
  bool compare(int value) => value < 0;
}

class _LessThanOrEqualsPredicate<T> extends _ComparingPredicate<T> {
  _LessThanOrEqualsPredicate(Type type, String field, dynamic expression) : super(type, field, expression);

  @override
  bool compare(int value) => value <= 0;
}

class _BetweenPredicate<T> extends Predicate<T> {
  final Predicate<T> _lowerBound;
  final Predicate<T> _upperBound;

  _BetweenPredicate(Type type, String field, dynamic value1, dynamic value2)
      : _lowerBound = _GreaterThanPredicate(type, field, value1),
        _upperBound = _LessThanPredicate(type, field, value2);

  @override
  bool evaluate(T object) => _lowerBound.evaluate(object) && _upperBound.evaluate(object);
}

abstract class _ExpressionPredicate<T, E> extends Predicate<T> {
  Type type;
  String field;
  E expression;

  _ExpressionPredicate(this.type, this.field, this.expression);

  valueOf(T object) {
    var typeReflection = TypeReflection(type);
    dynamic currentValue = object;
    for (var subField in field.split('.')) {
      var fieldReflection = typeReflection.field(subField);
      if (fieldReflection == null) {
        throw Exception('Field not found: $typeReflection.$subField');
      }
      currentValue = fieldReflection.value(currentValue);
      if (currentValue == null) {
        return null;
      }
      typeReflection = TypeReflection.fromInstance(currentValue);
    }
    return currentValue;
  }
}
