import 'package:collection/collection.dart';
import 'package:reflective/reflective.dart';

abstract class Box {
  bool get persistent => true;

  Future store(Object entity);

  Future storeAll(List<Object> entities) async {
    for (var entity in entities) {
      await store(entity);
    }
  }

  static keyOf(Object entity) {
    var type = TypeReflection.fromInstance(entity);
    var key = <String, dynamic>{};
    type.fieldsWith(Key).values.forEach((field) => key[field.name] = field.value(entity));
    if (key.isEmpty) throw Exception('No fields found with @key in $type');
    return key.length == 1 ? key.values.first : Composite(key);
  }

  Future<T> find<T>(key, [Type type]);

  QueryStep<T> selectFrom<T>([Type type]);

  SelectStep select(List<Field> fields);

  Future deleteAll<T>([Type type]);

  Future close();
}

abstract class SelectStep {
  QueryStep from(Type type);
}

abstract class QueryStep<T> extends ExpectationStep<T> {
  WhereStep<T> where(String field);

  OrderByStep<T> orderBy(String field);

  WhereStep<T> and(String field);

  WhereStep<T> or(String field);
}

abstract class WhereStep<T> {
  WhereStep<T> not();

  QueryStep<T> like(String expression);

  QueryStep<T> equals(dynamic value);

  QueryStep<T> gt(dynamic value);

  QueryStep<T> gte(dynamic value);

  QueryStep<T> lt(dynamic value);

  QueryStep<T> lte(dynamic value);

  QueryStep<T> between(dynamic value1, dynamic value2);
}

abstract class OrderByStep<T> {
  ExpectationStep<T> ascending();

  ExpectationStep<T> descending();
}

typedef T Mapper<T>(dynamic input);

abstract class ExpectationStep<T> {
  ExpectationStep<M> mapTo<M>([Mapper<M> mapper]) => _MappingStep(this, mapper ?? _typeMapper<M>());

  Stream<T> stream({int limit, int offset});

  Future<List<T>> list({int limit = 1000000, int offset = 0}) async => stream(limit: limit, offset: offset).toList();

  Future<T> unique();

  Mapper<M> _typeMapper<M>() {
    if (M != dynamic) {
      var reflection = TypeReflection<M>();
      return (record) => Conversion.convert(record).to(reflection.rawType);
    }
    return (record) => record;
  }
}

class _MappingStep<T> extends ExpectationStep<T> {
  final ExpectationStep<dynamic> _wrapped;
  final Mapper<T> _mapper;

  _MappingStep(this._wrapped, this._mapper);

  @override
  ExpectationStep<M> mapTo<M>([M Function(T p1) mapper]) => _MappingStep(this, mapper);

  @override
  Stream<T> stream({int limit, int offset}) => _wrapped.stream(limit: limit, offset: offset).map(_mapper);

  @override
  Future<T> unique() => stream().first;
}

abstract class Predicate<T> {
  bool evaluate(T object);

  Predicate<T> not() => NotPredicate(this);

  or(Predicate<T> other) => OrPredicate([this, other]);

  and(Predicate<T> other) => AndPredicate([this, other]);
}

class AndPredicate<T> extends Predicate<T> {
  final List<Predicate<T>> _predicates;

  AndPredicate(this._predicates);

  @override
  bool evaluate(T object) => _predicates.every((predicate) => predicate.evaluate(object));
}

class OrPredicate<T> extends Predicate<T> {
  final List<Predicate<T>> _predicates;

  OrPredicate(this._predicates);

  @override
  bool evaluate(T object) => _predicates.any((predicate) => predicate.evaluate(object));
}

class NotPredicate<T> extends Predicate<T> {
  final Predicate<T> _predicate;

  NotPredicate(this._predicate);

  @override
  bool evaluate(T object) => !_predicate.evaluate(object);
}

class Composite {
  Map<String, dynamic> components;

  Composite(this.components);

  int get hashCode => components.entries
      .map((entry) => 11 * entry.key.hashCode + 19 * entry.value.hashCode)
      .reduce((int c1, int c2) => c1 + 17 * c2);

  bool operator ==(other) {
    if (other == null || !(other is Composite)) {
      return false;
    }
    return MapEquality().equals(components, other.components);
  }
}

class Key {
  const Key();
}

const key = const Key();

class Field {
  final String name;
  final String alias;

  Field(this.name, this.alias);
}

$(String name, {String alias}) => Field(name, alias ?? name);
