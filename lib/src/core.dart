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

  Future<T> find<T>(key);

  QueryStep<T> selectFrom<T>();

  Future deleteAll<T>();

  Future close();
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

abstract class ExpectationStep<T> {
  Stream<T> stream();

  Future<List<T>> list() async => stream().toList();

  Future<T> unique();
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
