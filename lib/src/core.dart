library box.core;

import 'package:collection/collection.dart';

/// A Box represents a storage, be it a file, a database or simply in-memory storage.
abstract class Box {
  final Registry registry;

  Box(this.registry);

  /// Returns true if this Box persists stored entities.
  bool get persistent => true;

  /// Stores an entity in this Box. The entity is persisted if this Box implementation is persistent.
  Future<K> store<K>(dynamic entity);

  /// Stores all specified entities in this Box.
  Future<List<K>> storeAll<K>(Iterable<dynamic> entities) async {
    var keys = <K>[];
    for (var entity in entities) {
      keys.add(await store(entity));
    }
    return keys;
  }

  /// Inserts all specified entities of the same type in this Box.
  Future insertAll<T>(Iterable<T> entities) =>
      storeAll(entities); // TODO: implement correctly

  /// Returns the key of the specified entity.
  dynamic keyOf(dynamic entity) =>
      registry.lookup(entity.runtimeType).getKey(entity);

  /// Finds an entity of the specified type by primary key.
  Future<T?> find<T>(dynamic key, [Type type]);

  /// Starts a new query selecting all fields from the specified type. SQL: SELECT * FROM <type>
  QueryStep<T> selectFrom<T>([Type type, String alias]);

  /// Starts a new query selecting the specified fields from the specified type. SQL equivalent SELECT <fields>
  SelectStep select(List<Field> fields);

  /// Deletes all entities of the specified type from this Box.
  Future deleteAll<T>([Type type]);

  /// Starts a new delete statement with a condition. SQL equivalent DELETE FROM <type>
  DeleteStep<T> deleteFrom<T>([Type type]);

  /// Closes the underlying connection of this Box. After closing, a Box can no longer be used.
  Future close();

  /// Returns true if this Box implementation supports composite primary keys.
  bool get supportsCompositeKey => true;

  /// Returns true if this Box implementation supports like conditions.
  bool get supportsLike => true;

  /// Returns true if this Box implementation supports the NOT operator.
  bool get supportsNot => true;

  /// Returns true if this Box implementation supports the OR operator.
  bool get supportsOr => true;

  /// Returns true if this Box implementation supports IN conditions.
  bool get supportsIn => true;
}

abstract class SelectStep {
  /// Specifies the type of this query to select from. SQL: FROM <table>
  QueryStep from(Type type, [String alias]);
}

abstract class DeleteStep<T> {
  /// Adds a condition to this delete for the specified field. SQL: WHERE <field>
  WhereStep<T, DeleteStep<T>> where(String field);

  /// Adds another condition to this delete for the specified field. Only results that match both the previous condition
  /// and the next will be deleted. SQL: AND <field>
  WhereStep<T, DeleteStep<T>> and(String field);

  /// Adds another condition to this delete for the specified field. Both results that match the previous condition
  /// and the next will be deleted. SQL: OR <field>
  WhereStep<T, DeleteStep<T>> or(String field);

  /// Executes the delete with the specified conditions.
  Future execute();
}

abstract class QueryStep<T> extends ExpectationStep<T> {
  /// Adds a condition to this query for the specified field. SQL: WHERE <field>
  WhereStep<T, QueryStep<T>> where(String field);

  /// Adds a sort order to this query for the specified field. SQL: ORDER BY <field>
  OrderByStep<T> orderBy(String field);

  /// Adds another condition to this query for the specified field. Only results that match both the previous condition
  /// and the next will be returned from the query. SQL: AND <field>
  WhereStep<T, QueryStep<T>> and(String field);

  /// Adds another condition to this query for the specified field. Both results that match the previous condition
  /// and the next will be returned from the query. SQL: OR <field>
  WhereStep<T, QueryStep<T>> or(String field);

  JoinStep<T> innerJoin(Type type, [String alias]);
}

abstract class JoinStep<T> {
  WhereStep<T, QueryStep<T>> on(String field);
}

abstract class WhereStep<T, S> {
  /// Negates the next condition. SQL: NOT <condition>
  WhereStep<T, S> not();

  /// Query condition that matches parts of a string using % as wildcard. SQL: LIKE <expression>
  S like(String expression);

  /// Query condition that matches all values equal to the specified value. SQL: =
  S equals(dynamic value);

  /// Query condition that matches all values greater than the specified value. SQL: >
  S gt(dynamic value);

  /// Query condition that matches all values greater than or equal to the specified value. SQL: >=
  S gte(dynamic value);

  /// Query condition that matches all values less than the specified value. SQL: <
  S lt(dynamic value);

  /// Query condition that matches all values less than or equal to the specified value. SQL: <=
  S lte(dynamic value);

  /// Query condition that matches all values greater than the first value and less than the second value.
  /// SQL: BETWEEN <value1> AND <value2>
  S between(dynamic value1, dynamic value2);

  /// Query condition that matches any value in the specified list of values. SQL: IN(<values>)
  S in_(Iterable<dynamic> values);

  /// Query condition that matches any string that contains the specified value.
  S contains(dynamic value);
}

abstract class OrderByStep<T> {
  /// Returns the values in ascending order for this sort criteria.
  ExpectationStep<T> ascending();

  /// Returns the values in descending order for this sort criteria.
  ExpectationStep<T> descending();
}

/// A mapping function that maps records to the specified type.
typedef Mapper<T> = T? Function(dynamic input);

abstract class ExpectationStep<T> {
  /// Maps resulting records using the specified mapping function.
  ExpectationStep<M> mapTo<M>([Mapper<M>? mapper]) =>
      _MappingStep(this, mapper ?? _typeMapper<M>());

  /// Returns a stream of results, optionally limited by:
  ///   offset: starts returning results from the specified offset and skips all records before. SQL: OFFSET <number>
  ///   limit: limits the number of results returned. SQL: LIMIT <number>
  Stream<T> stream({int limit = 1000000, int offset = 0});

  /// Returns a list of results, optionally limited by:
  ///   offset: starts returning results from the specified offset and skips all records before. SQL: OFFSET <number>
  ///   limit: limits the number of results returned. SQL: LIMIT <number>
  Future<List<T>> list({int limit = 1000000, int offset = 0}) async =>
      stream(limit: limit, offset: offset).toList();

  /// Returns a single result.
  Future<T?> unique() => stream(limit: 1, offset: 0).firstOrNull;

  Mapper<M?> _typeMapper<M>() =>
      (map) => box.registry.lookup<M>().deserialize(map);

  Box get box;
}

class _MappingStep<T> extends ExpectationStep<T> {
  final ExpectationStep<dynamic> _wrapped;
  final Mapper<T> _mapper;

  _MappingStep(this._wrapped, this._mapper);

  @override
  ExpectationStep<M> mapTo<M>([Mapper<M>? mapper]) =>
      _MappingStep(this, mapper ?? (v) => v);

  @override
  Stream<T> stream({int limit = 1000000, int offset = 0}) =>
      _wrapped.stream(limit: limit, offset: offset).map(_mapper).whereNotNull();

  @override
  Future<T?> unique() => stream().firstOrNull;

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
  bool evaluate(T object) =>
      _predicates.every((predicate) => predicate.evaluate(object));
}

class OrPredicate<T> extends Predicate<T> {
  final List<Predicate<T>> _predicates;

  OrPredicate(this._predicates);

  @override
  bool evaluate(T object) =>
      _predicates.any((predicate) => predicate.evaluate(object));
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

/// Metadata that indicates a field is a primary key.
class Key {
  const Key();
}

const key = Key();

/// Metadata that indicates a class is an entity.
class Entity {
  const Entity();
}

const entity = Entity();

enum Direction { ascending, descending }

const index = Index();

class Index {
  final List<IndexField> fields;

  const Index([this.fields = const []]);
}

class IndexField {
  final String name;
  final Direction direction;

  const IndexField(this.name, [this.direction = Direction.ascending]);
}

class Field {
  final String name;
  final String alias;

  Field(this.name, this.alias);

  dynamic resolve(Map<String, dynamic> map) {
    var path = name.split('.');
    dynamic result = map;
    for (var part in path) {
      result = result[part];
    }
    return result;
  }
}

Field $(String name, {String? alias}) => Field(name, alias ?? name);

typedef FieldAccessor<T> = dynamic Function(T entity);

/// Base class that holds information about an entity.
abstract class EntitySupport<T> {
  final FieldAccessor<T> keyAccessor;
  final Map<String, FieldAccessor<T>> fieldAccessors;
  final Map<String, Type> fieldTypes;
  final List<String> keyFields;
  final List<Index> indexes;
  final String name;
  Registry? registry;

  EntitySupport({
    required this.name,
    required this.keyAccessor,
    required this.fieldAccessors,
    required this.keyFields,
    required this.fieldTypes,
    this.indexes = const [],
  });

  dynamic getKey(T entity) => keyAccessor(entity);

  dynamic getFieldValue(String fieldName, T entity) {
    var fieldAccessor = fieldAccessors[fieldName];
    if (fieldAccessor == null) {
      throw 'No such field "$fieldName" on entity $name';
    }
    return fieldAccessor(entity);
  }

  bool isKey(String field) => keyFields.contains(field);

  List<String> get fields => fieldAccessors.keys.toList();

  T? deserialize(Map<String, dynamic>? map);

  Map<String, dynamic> serialize(T entity);

  DateTime? deserializeDateTime(dynamic input) => input != null
      ? input is DateTime
          ? input
          : DateTime.parse(input)
      : null;

  E? deserializeEntity<E>(Map<String, dynamic>? map) =>
      map != null ? registry!.lookup<E>().deserialize(map) : null;

  Map<String, dynamic>? serializeEntity<E>(E? entity) =>
      entity != null ? registry!.lookup<E>().serialize(entity) : null;

  String? serializeEnum(dynamic value) =>
      value?.toString().substring(value.toString().indexOf('.') + 1);

  E? deserializeEnum<E>(String? value, List<E> values) {
    if (value == null) {
      return null;
    }
    var filtered = values.where((element) => serializeEnum(element) == value);
    if (filtered.isEmpty) {
      throw ArgumentError(
          'Could not deserialize "$value" as $E, no matching value found');
    }
    return filtered.first;
  }
}

/// Holds entity information fox Box implementations.
/// Every Box implementation requires a registry.
class Registry {
  final Map<Type, EntitySupport> entries = {};

  /// Register the generated EntitySupport for entity to use with Box.
  EntitySupport<T> register<T>(EntitySupport<T> support) {
    entries[T] = support;
    support.registry = this;
    return support;
  }

  /// Lookup the EntitySupport for a type.
  EntitySupport<T> lookup<T>([Type? type]) {
    var support = entries[type ?? T];
    if (support == null) {
      throw 'No registry entry found for ${type ?? T}. To fix this:\n'
          ' 1. Make sure the class is annotated with @entity\n'
          ' 2. Make sure box_generator is added to dev_dependencies in pubspec.yaml\n'
          ' 3. Run "pub run build_runner build" again'
          ' 4. Add the generated Box support to the registry: Registry()..register(${type ?? T}\$BoxSupport())';
    }
    return support as EntitySupport<T>;
  }

  /// Returns the value of the field with the specified fieldName from the specified entity.
  dynamic getFieldValue(String fieldName, dynamic entity) {
    dynamic currentValue = entity;
    for (var subField in fieldName.split('.')) {
      currentValue = lookup(currentValue.runtimeType)
          .getFieldValue(subField, currentValue);
      if (currentValue == null) {
        return null;
      }
    }
    return currentValue;
  }
}

extension FirstOrNull<E> on Stream<E> {
  Future<E?> get firstOrNull async {
    try {
      return await first;
    } on StateError {
      return null;
    }
  }
}

extension WhereNotNull<E> on Stream<E?> {
  Stream<E> whereNotNull() =>
      where((element) => element != null).map((element) => element as E);
}
