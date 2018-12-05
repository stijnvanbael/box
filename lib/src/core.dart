import 'package:collection/collection.dart';
import 'package:reflective/reflective.dart';

abstract class Box {
  Future store(Object entity);

  static keyOf(Object entity) {
    TypeReflection type = TypeReflection.fromInstance(entity);
    Iterable key =
        type.fieldsWith(Key).values.map((field) => field.value(entity));
    if (key.isEmpty) throw Exception('No fields found with @key in $type');
    return key.length == 1 ? key.first : Composite(key);
  }

  Future<T> find<T>(key);

  QueryStep<T> selectFrom<T>();
}

abstract class QueryStep<T> extends ExpectationStep<T> {
  WhereStep<T> where(String field);

  OrderByStep<T> orderBy(String field);
}

abstract class WhereStep<T> {
  WhereStep<T> not();

  QueryStep<T> like(String expression);

  QueryStep<T> equals(String expression);
}

abstract class OrderByStep<T> {
  ExpectationStep<T> ascending();

  ExpectationStep<T> descending();
}

abstract class ExpectationStep<T> {
  Stream<T> stream();

  Future<List<T>> list() async => stream().toList();

  Predicate<T> createPredicate();

  Future<Optional<T>> unique();
}

abstract class Predicate<T> {
  bool evaluate(T object);
}

class Composite {
  Iterable components;

  Composite(this.components);

  int get hashCode => components
      .map((c) => 11 * c.hashCode)
      .reduce((int c1, int c2) => c1 + 17 * c2);

  bool operator ==(other) {
    if (other == null || !(other is Composite)) {
      return false;
    }
    return IterableEquality().equals(components, other.components);
  }
}

class Key {
  const Key();
}

const key = const Key();
