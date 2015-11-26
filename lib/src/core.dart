part of box;

class Box {
  Box();

  factory Box.file(String filename) => new FileBox(filename);

  Map<String, Map> _entities = {
  };

  Future store(Object entity) {
    TypeReflection type = new TypeReflection.fromInstance(entity);
    return _entitiesFor(type).then((entities) {
      entities[keyOf(entity)] = entity;
    });
  }

  static keyOf(Object entity) {
    TypeReflection type = new TypeReflection.fromInstance(entity);
    Iterable key = type
        .fieldsWith(Key)
        .values
        .map((field) => field.value(entity));
    if (key.isEmpty)
      throw new Exception('No fields found with @key in $type');
    return key.length == 1 ? key.first : new Composite(key);
  }

  Future find(Type type, key) async {
    return _entitiesFor(new TypeReflection(type)).then((entitiesForType) {
      return entitiesForType != null ? entitiesForType[key is Iterable
          ? new Composite(key)
          : key] : null;
    });
  }

  Stream _query(TypeReflection type, Predicate predicate, Ordering ordering) {
    return new Stream.fromFuture(_entitiesFor(type).then((entities) {
      List list = new List.from(entities.values.where((item) =>
      predicate != null ? predicate.evaluate(item) : true));
      if (ordering != null)
        list.sort((object1, object2) => ordering.compare(object1, object2));
      return list;
    })).expand((list) => list);
  }

  QueryStep selectFrom(Type type) {
    return new QueryStep(type, this);
  }

  Future<Map> _entitiesFor(TypeReflection type) {
    _entities.putIfAbsent(type.name, () => new Map());
    return new Future.value(_entities[type.name]);
  }
}

class QueryStep<T> extends ExpectationStep<T> {
  QueryStep(Type type, Box box) : super(type, box);

  QueryStep.withPredicate(QueryStep<T> query, Predicate<T> predicate)
      : super(query.type, query.box, predicate);

  WhereStep<T> where(String field) => new WhereStep(field, this);

  OrderByStep<T> orderBy(String field) => new OrderByStep(field, this);
}

class NotQueryStep<T> extends QueryStep<T> {
  QueryStep<T> query;

  NotQueryStep(QueryStep<T> query) : super(query.type, query.box) {
    this.query = query;
  }

  Predicate<T> createPredicate() => new NotPredicate(query.predicate);
}

class WhereStep<T> {
  String field;
  QueryStep<T> query;
  bool _not = false;

  WhereStep(this.field, this.query);

  WhereStep<T> not() {
    _not = true;
    return this;
  }

  QueryStep<T> like(String expression) => _wrap(new QueryStep.withPredicate(
      query, new LikePredicate(query.type, field, expression)));

  QueryStep<T> equals(String expression) => _wrap(new QueryStep.withPredicate(
      query, new EqualsPredicate(query.type, field, expression)));

  QueryStep<T> _wrap(QueryStep<T> query) =>
      _not ? new NotQueryStep(query) : query;
}

class OrderByStep<T> {
  QueryStep<T> query;
  String field;

  OrderByStep(this.field, this.query);

  ExpectationStep<T> ascending() => new ExpectationStep(
      query.type, query.box, query.createPredicate(),
      new Ascending(query.type, field));

  ExpectationStep<T> descending() => new ExpectationStep(
      query.type, query.box, query.createPredicate(),
      new Descending(query.type, field));
}

class ExpectationStep<T> {
  Type type;
  Box box;
  Predicate<T> predicate;
  Ordering<T> ordering;

  ExpectationStep(this.type, this.box, [this.predicate, this.ordering]);

  Stream<T> list() {
    return box._query(new TypeReflection(type), predicate, ordering);
  }

  Predicate<T> createPredicate() => predicate;

  Future<Optional<T>> unique() {
    return list().first.then((item) => new Optional.of(item))
        .catchError((e) => empty, test: (e) => e is StateError);
  }
}

class Ascending<T> extends Ordering<T> {
  Ascending(Type type, String field) : super(type, field);

  int compare(T object1, T object2) {
    var value1 = valueOf(object1);
    var value2 = valueOf(object2);
    return value1.toString().compareTo(value2);
  }
}

class Descending<T> extends Ordering<T> {
  Descending(Type type, String field) : super(type, field);

  int compare(T object1, T object2) {
    var value1 = valueOf(object1);
    var value2 = valueOf(object2);
    return -value1.toString().compareTo(value2);
  }
}

abstract class Ordering<T> {
  final Type type;
  final String field;
  FieldReflection fieldReflection;

  Ordering(this.type, this.field) {
    TypeReflection typeReflection = new TypeReflection(type);
    this.fieldReflection = typeReflection.field(field);
    if (fieldReflection == null) {
      throw new Exception('Field not found: $typeReflection.' + field);
    }
  }

  int compare(T object1, T object2);

  valueOf(T object) {
    return fieldReflection.value(object);
  }
}

class LikePredicate<T> extends ExpressionPredicate<T, RegExp> {

  LikePredicate(Type type, String field, String expression)
      : super(
      type, field, new RegExp(expression.replaceAll(new RegExp(r'%'), '.*')));

  bool evaluate(T object) {
    var value = valueOf(object);
    return value != null && expression.hasMatch(value.toString());
  }
}

class EqualsPredicate<T> extends ExpressionPredicate<T, String> {

  EqualsPredicate(Type type, String field, String expression)
      : super(type, field, expression);

  bool evaluate(T object) {
    var value = valueOf(object);
    return value != null && expression == value.toString();
  }
}

abstract class ExpressionPredicate<T, E> extends Predicate<T> {
  Type type;
  String field;
  E expression;

  ExpressionPredicate(this.type, this.field, this.expression);

  valueOf(T object) {
    TypeReflection typeReflection = new TypeReflection(type);
    FieldReflection fieldReflection = typeReflection.field(field);
    if (fieldReflection == null) {
      throw new Exception('Field not found: $typeReflection.' + field);
    }
    return fieldReflection.value(object);
  }
}

abstract class Predicate<T> {
  bool evaluate(T object);
}

class NotPredicate<T> extends Predicate<T> {
  Predicate<T> delegate;

  NotPredicate(this.delegate);

  bool evaluate(T object) => !delegate.evaluate(object);
}

class Composite {
  Iterable components;

  Composite(this.components);

  int get hashCode =>
      components.map((c) => 11 * c.hashCode).reduce((int c1, int c2) => c1 +
          17 * c2);

  bool operator ==(other) {
    if (other == null || !(other is Composite)) {
      return false;
    }
    return new IterableEquality().equals(components, other.components);
  }
}

class Key {
  const Key();
}

const key = const Key();



