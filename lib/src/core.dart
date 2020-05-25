library box.core;

import 'package:collection/collection.dart';

abstract class Box {
  final Registry registry;

  Box(this.registry);

  bool get persistent => true;

  Future store(dynamic entity);

  Future storeAll(List<dynamic> entities) async {
    for (var entity in entities) {
      await store(entity);
    }
  }

  dynamic keyOf(dynamic entity) => registry.lookup(entity.runtimeType).getKey(entity);

  Future<T> find<T>(dynamic key, [Type type]);

  QueryStep<T> selectFrom<T>([Type type]);

  SelectStep select(List<Field> fields);

  Future deleteAll<T>([Type type]);

  Future close();

  bool get compositeKeySupported => true;

  bool get likeSupported => true;

  bool get notSupported => true;

  bool get orSupported => true;

  bool get oneOfSupported => true;
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

  QueryStep<T> oneOf(List<dynamic> values);

  QueryStep<T> contains(dynamic value);
}

abstract class OrderByStep<T> {
  ExpectationStep<T> ascending();

  ExpectationStep<T> descending();
}

typedef Mapper<T> = T Function(dynamic input);

abstract class ExpectationStep<T> {
  ExpectationStep<M> mapTo<M>([Mapper<M> mapper]) => _MappingStep(this, mapper ?? _typeMapper<M>());

  Stream<T> stream({int limit, int offset});

  Future<List<T>> list({int limit = 1000000, int offset = 0}) async => stream(limit: limit, offset: offset).toList();

  Future<T> unique();

  Mapper<M> _typeMapper<M>() => (map) => box.registry.lookup<M>().deserialize(map);

  Box get box;
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

  @override
  Box get box => _wrapped.box;
}

abstract class Predicate<T> {
  bool evaluate(T object);

  Predicate<T> not() => NotPredicate(this);

  OrPredicate<T> or(Predicate<T> other) => OrPredicate([this, other]);

  AndPredicate<T> and(Predicate<T> other) => AndPredicate([this, other]);
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

  @override
  int get hashCode => components.entries
      .map((entry) => 11 * entry.key.hashCode + 19 * entry.value.hashCode)
      .reduce((int c1, int c2) => c1 + 17 * c2);

  @override
  bool operator ==(other) {
    if (!(other is Composite)) {
      return false;
    }
    return MapEquality().equals(components, other.components);
  }
}

class Key {
  const Key();
}

const key = Key();

class Entity {
  const Entity();
}

const entity = Entity();

class Field {
  final String name;
  final String alias;

  Field(this.name, this.alias);
}

Field $(String name, {String alias}) => Field(name, alias ?? name);

typedef Deserializer<T> = T Function(Map map);

typedef FieldAccessor<T> = dynamic Function(T entity);

abstract class EntitySupport<T> {
  final FieldAccessor<T> _keyAccessor;
  final Deserializer<T> _deserializer;
  final Map<String, FieldAccessor<T>> _fieldAccessors;
  final List<String> keyFields;
  final String name;

  EntitySupport(this.name, this._keyAccessor, this._deserializer, this._fieldAccessors, this.keyFields);

  dynamic getKey(T entity) => _keyAccessor(entity);

  T deserialize(Map map) => map != null ? _deserializer(map) : null;

  dynamic getFieldValue(String fieldName, T entity) {
    var fieldAccessor = _fieldAccessors[fieldName];
    if (fieldAccessor == null) {
      throw 'No such field "$fieldName" on entity $name';
    }
    return fieldAccessor(entity);
  }

  bool isKey(String field) => keyFields.contains(field);
}

class Registry {
  final Map<Type, EntitySupport> _entries = {};

  EntitySupport<T> register<T>(EntitySupport<T> support) {
    _entries[T] = support;
    return support;
  }

  EntitySupport<T> lookup<T>([Type type]) {
    var support = _entries[type ?? T];
    if (support == null) {
      throw 'No entry found for ${type ?? T}. To fix this:\n'
          ' 1. Make sure the class is annotated with @entity\n'
          ' 2. Make sure box_generator is added to dev_dependencies in pubspec.yaml\n'
          ' 3. Run "pub run build_runner build" again';
    }
    return support;
  }

  dynamic getFieldValue(String fieldName, dynamic entity) {
    dynamic currentValue = entity;
    for (var subField in fieldName.split('.')) {
      currentValue = lookup(currentValue.runtimeType).getFieldValue(subField, currentValue);
      if (currentValue == null) {
        return null;
      }
    }
    return currentValue;
  }
}
