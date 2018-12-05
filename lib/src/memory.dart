import 'package:box/box.dart';
import 'package:reflective/reflective.dart';

class MemoryBox extends Box {
  final Map<String, Map> entities = {};

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
      return entitiesForType != null
          ? entitiesForType[key is Iterable ? Composite(key) : key]
          : null;
    });
  }

  Stream<T> _query<T>(
      TypeReflection<T> type, Predicate predicate, _Ordering ordering) {
    return Stream.fromFuture(entitiesFor(type).then((entities) {
      List<T> list = List.from(entities.values.where(
          (item) => predicate != null ? predicate.evaluate(item) : true));
      if (ordering != null)
        list.sort((object1, object2) => ordering.compare(object1, object2));
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
}

class _QueryStep<T> extends _ExpectationStep<T> implements QueryStep<T> {
  _QueryStep(Box box) : super(box);

  _QueryStep.withPredicate(_QueryStep<T> query, Predicate<T> predicate)
      : super(query.box, predicate);

  Type get type => T;

  @override
  WhereStep<T> where(String field) => _WhereStep(field, this);

  @override
  OrderByStep<T> orderBy(String field) => _OrderByStep(field, this);
}

class _NotQueryStep<T> extends _QueryStep<T> {
  _QueryStep<T> query;

  _NotQueryStep(_QueryStep<T> query) : super(query.box) {
    this.query = query;
  }

  Predicate<T> createPredicate() => _NotPredicate(query.predicate);
}

class _ExpectationStep<T> extends ExpectationStep<T> {
  final MemoryBox box;
  final Predicate<T> predicate;
  final _Ordering<T> ordering;

  _ExpectationStep(this.box, [this.predicate, this.ordering]);

  @override
  Stream<T> stream() {
    return box._query(TypeReflection<T>(), predicate, ordering);
  }

  @override
  Predicate<T> createPredicate() => predicate;

  @override
  Future<Optional<T>> unique() {
    return stream()
        .first
        .then((item) => Optional.of(item))
        .catchError((e) => empty, test: (e) => e is StateError);
  }
}

class _WhereStep<T> implements WhereStep<T> {
  final String field;
  final _QueryStep<T> query;
  bool _not = false;

  _WhereStep(this.field, this.query);

  @override
  WhereStep<T> not() {
    _not = true;
    return this;
  }

  @override
  QueryStep<T> like(String expression) => _wrap(_QueryStep.withPredicate(
      query, _LikePredicate(query.type, field, expression)));

  @override
  QueryStep<T> equals(String expression) => _wrap(_QueryStep.withPredicate(
      query, _EqualsPredicate(query.type, field, expression)));

  QueryStep<T> _wrap(_QueryStep<T> query) =>
      _not ? _NotQueryStep(query) : query;
}

class _OrderByStep<T> implements OrderByStep<T> {
  _QueryStep<T> query;
  String field;

  _OrderByStep(this.field, this.query);

  @override
  ExpectationStep<T> ascending() => _ExpectationStep(
      query.box, query.createPredicate(), _Ascending(query.type, field));

  @override
  ExpectationStep<T> descending() => _ExpectationStep(
      query.box, query.createPredicate(), _Descending(query.type, field));
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
      : super(type, field, RegExp(expression.replaceAll(RegExp(r'%'), '.*')));

  @override
  bool evaluate(T object) {
    var value = valueOf(object);
    return value != null && expression.hasMatch(value.toString());
  }
}

class _EqualsPredicate<T> extends _ExpressionPredicate<T, String> {
  _EqualsPredicate(Type type, String field, String expression)
      : super(type, field, expression);

  @override
  bool evaluate(T object) {
    var value = valueOf(object);
    return value != null && expression == value.toString();
  }
}

abstract class _ExpressionPredicate<T, E> extends Predicate<T> {
  Type type;
  String field;
  E expression;

  _ExpressionPredicate(this.type, this.field, this.expression);

  valueOf(T object) {
    TypeReflection typeReflection = TypeReflection(type);
    FieldReflection fieldReflection = typeReflection.field(field);
    if (fieldReflection == null) {
      throw Exception('Field not found: $typeReflection.' + field);
    }
    return fieldReflection.value(object);
  }
}

class _NotPredicate<T> extends Predicate<T> {
  Predicate<T> delegate;

  _NotPredicate(this.delegate);

  @override
  bool evaluate(T object) => !delegate.evaluate(object);
}
