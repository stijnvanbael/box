library box.memory;

import 'package:box/box.dart';

class MemoryBox extends Box {
  final Map<String, Map> entities = {};

  MemoryBox(Registry registry) : super(registry);

  @override
  bool get persistent => false;

  @override
  Future<K> store<K>(dynamic entity) async {
    var entities = await entitiesFor(entity.runtimeType);
    entities[keyOf(entity)] = entity;
    return keyOf(entity) as K;
  }

  @override
  Future<T?> find<T>(dynamic key, [Type? type]) async {
    return entitiesFor(type ?? T).then((entitiesForType) {
      return entitiesForType[
          key is Map ? Composite(key as Map<String, dynamic>) : key];
    });
  }

  Stream<T> _query<T>(Type type, Predicate? predicate, _Ordering? ordering) {
    return Stream.fromFuture(entitiesFor(type).then((entities) {
      var list = List<T>.from(entities.values.where(
          (item) => predicate != null ? predicate.evaluate(item) : true));
      if (ordering != null) {
        list.sort((object1, object2) => ordering.compare(object1, object2));
      }
      return list;
    })).expand((list) => list);
  }

  @override
  SelectStep select(List<Field> fields) => _SelectStep(this, fields);

  @override
  _QueryStep<T> selectFrom<T>([Type? type, String? alias]) =>
      _QueryStep<T>(this, type);

  Future<Map> entitiesFor(Type type) {
    var entitySupport = registry.lookup(type);
    entities.putIfAbsent(entitySupport.name, () => {});
    return Future.value(entities[entitySupport.name]);
  }

  @override
  Future deleteAll<T>([Type? type]) async =>
      (await entitiesFor(type ?? T)).clear();

  @override
  Future close() async {}

  @override
  DeleteStep<T> deleteFrom<T>([Type? type]) => _DeleteStep<T>(this, type ?? T);

  @override
  UpdateStep<T> update<T>([Type? type]) {
    // TODO: implement update
    throw UnimplementedError();
  }
}

class _DeleteStep<T> extends _TypedStep<T, _DeleteStep<T>>
    implements DeleteStep<T> {
  @override
  final MemoryBox box;
  @override
  final Type type;
  @override
  final Predicate<T>? predicate;

  _DeleteStep(this.box, this.type) : predicate = null;

  @override
  _DeleteStep<T> addPredicate(Predicate<T> predicate) =>
      _DeleteStep.withPredicate(this, predicate);

  _DeleteStep.withPredicate(_DeleteStep<T> step, Predicate<T> predicate)
      : box = step.box,
        type = step.type,
        predicate = predicate;

  @override
  Future execute() async => (await box.entitiesFor(type))
      .removeWhere((key, value) => predicate!.evaluate(value));

  @override
  WhereStep<T, DeleteStep<T>> where(String field) =>
      _DeleteWhereStep(field, this);
}

class _DeleteWhereStep<T> extends _WhereStep<T, _DeleteStep<T>> {
  _DeleteWhereStep(String field, _DeleteStep<T> delete) : super(field, delete);

  @override
  _DeleteStep<T> createNextStep(Predicate<T> predicate) =>
      _DeleteStep<T>.withPredicate(step, combine(predicate));
}

class _SelectStep implements SelectStep {
  final MemoryBox _box;
  final List<Field> _fields;

  _SelectStep(this._box, this._fields);

  @override
  _QueryStep from(Type type, [String? alias]) =>
      _QueryStep(_box, type, _fields);
}

abstract mixin class _TypedStep<T, S extends _TypedStep<T, S>> {
  Type get type;

  MemoryBox get box;

  Predicate<T>? get predicate;

  WhereStep<T, S> and(String field) => _AndStep(field, this as S);

  WhereStep<T, S> or(String field) => _OrStep(field, this as S);

  S addPredicate(Predicate<T> predicate);
}

class _QueryStep<T> extends _ExpectationStep<T>
    with _TypedStep<T, _QueryStep<T>>
    implements QueryStep<T> {
  _QueryStep(MemoryBox box, [Type? type, List<Field>? selectFields])
      : super(box, type ?? T, selectFields);

  _QueryStep.withPredicate(_QueryStep<T> query, Predicate<T> predicate)
      : super(query.box, query.type, query.selectFields, predicate);

  @override
  WhereStep<T, QueryStep<T>> where(String field) =>
      _QueryWhereStep(field, this);

  @override
  OrderByStep<T> orderBy(String field) => _OrderByStep(field, this);

  @override
  JoinStep<T> innerJoin(Type type, [String? alias]) {
    // TODO: implement innerJoin
    throw UnimplementedError();
  }

  @override
  _QueryStep<T> addPredicate(Predicate<T> predicate) =>
      _QueryStep.withPredicate(this, predicate);
}

class _OrStep<T, S extends _TypedStep<T, S>> extends _WhereStep<T, S> {
  _OrStep(String field, S step) : super(field, step);

  @override
  Predicate<T> combine(Predicate<T> predicate) =>
      step.predicate != null ? step.predicate!.or(predicate) : predicate;
}

class _AndStep<T, S extends _TypedStep<T, S>> extends _WhereStep<T, S> {
  _AndStep(String field, S step) : super(field, step);

  @override
  Predicate<T> combine(Predicate<T> predicate) =>
      step.predicate != null ? step.predicate!.and(predicate) : predicate;
}

class _ExpectationStep<T> extends ExpectationStep<T> {
  @override
  final MemoryBox box;
  final Predicate<T>? predicate;
  final _Ordering<T>? ordering;
  final Type? _type;
  final List<Field>? selectFields;

  _ExpectationStep(this.box,
      [this._type, this.selectFields, this.predicate, this.ordering]);

  @override
  Stream<T> stream({int limit = 1000000, int offset = 0}) {
    return box
        ._query(type, predicate, ordering)
        .skip(offset)
        .take(limit)
        .map(_selectFields);
  }

  @override
  Future<T> unique() {
    return stream().first;
  }

  Type get type => _type ?? T;

  T _selectFields(dynamic record) {
    if (selectFields == null) {
      return record;
    }
    return {
      for (var field in selectFields!)
        field.alias: box.registry.getFieldValue(field.name, record)
    } as T;
  }
}

class _WhereStep<T, S extends _TypedStep<T, S>> implements WhereStep<T, S> {
  final String field;
  final S step;

  _WhereStep(this.field, this.step);

  Predicate<T> combine(Predicate<T> predicate) => predicate;

  @override
  WhereStep<T, S> not() => _NotStep<T, S>(this);

  @override
  S like(String expression) =>
      createNextStep(_LikePredicate(field, expression, step.box.registry));

  @override
  S equals(dynamic value) =>
      createNextStep(_EqualsPredicate(field, value, step.box.registry));

  @override
  S gt(dynamic value) =>
      createNextStep(_GreaterThanPredicate(field, value, step.box.registry));

  @override
  S gte(dynamic value) => createNextStep(
      _GreaterThanOrEqualsPredicate(field, value, step.box.registry));

  @override
  S lt(dynamic value) =>
      createNextStep(_LessThanPredicate(field, value, step.box.registry));

  @override
  S lte(dynamic value) => createNextStep(
      _LessThanOrEqualsPredicate(field, value, step.box.registry));

  @override
  S between(dynamic value1, dynamic value2) => createNextStep(
      _BetweenPredicate(field, value1, value2, step.box.registry));

  @override
  S in_(Iterable<dynamic> values) =>
      createNextStep(_InPredicate(field, values.toList(), step.box.registry));

  @override
  S contains(dynamic value) =>
      createNextStep(_ContainsPredicate(field, value, step.box.registry));

  S createNextStep(Predicate<T> predicate) =>
      step.addPredicate(combine(predicate));
}

class _QueryWhereStep<T> extends _WhereStep<T, _QueryStep<T>> {
  _QueryWhereStep(String field, _QueryStep<T> query) : super(field, query);

  @override
  _QueryStep<T> createNextStep(Predicate<T> predicate) =>
      _QueryStep<T>.withPredicate(step, combine(predicate));
}

class _NotStep<T, S extends _TypedStep<T, S>> extends _WhereStep<T, S> {
  _NotStep(_WhereStep<T, S> whereStep) : super(whereStep.field, whereStep.step);

  @override
  Predicate<T> combine(Predicate<T> predicate) => predicate.not();
}

class _OrderByStep<T> implements OrderByStep<T> {
  _QueryStep<T> query;
  String field;

  _OrderByStep(this.field, this.query);

  @override
  ExpectationStep<T> ascending() => _ExpectationStep(
      query.box,
      query.type,
      query.selectFields,
      query.predicate,
      _Ascending(query.type, field, query.box.registry));

  @override
  ExpectationStep<T> descending() => _ExpectationStep(
      query.box,
      query.type,
      query.selectFields,
      query.predicate,
      _Descending(query.type, field, query.box.registry));
}

class _Ascending<T> extends _Ordering<T> {
  _Ascending(Type type, String field, Registry registry)
      : super(type, field, registry);

  @override
  int compare(T object1, T object2) {
    var value1 = valueOf(object1);
    var value2 = valueOf(object2);
    return value1.toString().compareTo(value2);
  }
}

class _Descending<T> extends _Ordering<T> {
  _Descending(Type type, String field, Registry registry)
      : super(type, field, registry);

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
      : super(
            field,
            RegExp(expression.replaceAll('%', '.*'), caseSensitive: false),
            registry);

  @override
  bool evaluate(T object) {
    var value = valueOf(object);
    return value != null && expression.hasMatch(value.toString());
  }
}

class _EqualsPredicate<T, E> extends _ExpressionPredicate<T, E> {
  _EqualsPredicate(String field, E expression, Registry registry)
      : super(field, expression, registry);

  @override
  bool evaluate(T object) {
    var value = valueOf(object);
    return value != null && expression == value;
  }
}

abstract class _ComparingPredicate<T> extends _ExpressionPredicate<T, dynamic> {
  _ComparingPredicate(String field, dynamic expression, Registry registry)
      : super(field, expression, registry);

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
  _GreaterThanPredicate(String field, dynamic expression, Registry registry)
      : super(field, expression, registry);

  @override
  bool compare(int value) => value > 0;
}

class _GreaterThanOrEqualsPredicate<T> extends _ComparingPredicate<T> {
  _GreaterThanOrEqualsPredicate(
      String field, dynamic expression, Registry registry)
      : super(field, expression, registry);

  @override
  bool compare(int value) => value >= 0;
}

class _LessThanPredicate<T> extends _ComparingPredicate<T> {
  _LessThanPredicate(String field, dynamic expression, Registry registry)
      : super(field, expression, registry);

  @override
  bool compare(int value) => value < 0;
}

class _LessThanOrEqualsPredicate<T> extends _ComparingPredicate<T> {
  _LessThanOrEqualsPredicate(
      String field, dynamic expression, Registry registry)
      : super(field, expression, registry);

  @override
  bool compare(int value) => value <= 0;
}

class _BetweenPredicate<T> extends Predicate<T> {
  final Predicate<T> _lowerBound;
  final Predicate<T> _upperBound;

  _BetweenPredicate(
      String field, dynamic value1, dynamic value2, Registry registry)
      : _lowerBound = _GreaterThanPredicate(field, value1, registry),
        _upperBound = _LessThanPredicate(field, value2, registry);

  @override
  bool evaluate(T object) =>
      _lowerBound.evaluate(object) && _upperBound.evaluate(object);
}

class _InPredicate<T, E> extends _ExpressionPredicate<T, List<E>> {
  _InPredicate(String field, List<E> values, Registry registry)
      : super(field, values, registry);

  @override
  bool evaluate(T object) {
    var value = valueOf(object);
    return value != null && expression.contains(value.toString());
  }
}

class _ContainsPredicate<T, E> extends _ExpressionPredicate<T, E> {
  _ContainsPredicate(String field, E value, Registry registry)
      : super(field, value, registry);

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
