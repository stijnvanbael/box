part of box;

class Box {
  Box();

  factory Box.file(String filename) => new FileBox(filename);

  Map<String, Map> entities = {
  };

  store(Object entity) {
    String type = new TypeReflection.fromInstance(entity).name;
    entities.putIfAbsent(type, () => new Map());
    entities[type][keyOf(entity)] = entity;
  }

  static keyOf(Object entity) {
    TypeReflection type = new TypeReflection.fromInstance(entity);
    Iterable key = type.fieldsWith(Key).values.map((field) => field.value(entity));
    return key.length == 1 ? key.first : new Composite(key);
  }

  Future find(Type type, key) async {
    Map entitiesForType = entities[new TypeReflection(type).name];
    return entitiesForType != null ? entitiesForType[key is Iterable ? new Composite(key) : key] : null;
  }

  QueryStep query(Type type) {
    return new QueryStep(type, this);
  }
}

class QueryStep<T> extends ExpectationStep<T> {

  QueryStep(type, box) : super(type, box);

  QueryStep.withPredicate(QueryStep<T> query, Predicate<T> predicate) : super(query.type, query.box, predicate);

  WhereStep<T> where(String field) => new WhereStep(field, this);

  OrderByStep<T> orderBy(String field) => new OrderByStep(field, this);
}

class WhereStep<T> {
  String field;
  QueryStep<T> query;

  WhereStep(this.field, this.query);

  QueryStep<T> like(String expression) => new QueryStep.withPredicate(query, new LikePredicate(query.type, field, expression));

  QueryStep<T> equals(String expression) => new QueryStep.withPredicate(query, new EqualsPredicate(query.type, field, expression));
}

class OrderByStep<T> {
  QueryStep<T> query;
  String field;

  OrderByStep(this.field, this.query);

  ExpectationStep<T> ascending() => new ExpectationStep(query.type, query.box, query.predicate, new Ascending(query.type, field));
}

class ExpectationStep<T> {
  Type type;
  Box box;
  Predicate<T> predicate;
  Ordering<T> ordering;

  ExpectationStep(this.type, this.box, [this.predicate, this.ordering]);

  List<T> list() {
    List<T> list = new List.from(box.entities[new TypeReflection(type).name]
    .values
    .where((object) => predicate.evaluate(object)));
    list.sort((object1, object2) => ordering.compare(object1, object2));
    return list;
  }

  Optional<T> unique() {
    return new Optional.ofIterable(list());
  }
}

class Ascending<T> extends Ordering<T> {
  Ascending(Type type, String field) : super(type, field);

  int compare(T object1, T object2) {
    FieldReflection fieldReflection = new TypeReflection(type).fields[field];
    var value1 = fieldReflection.value(object1);
    var value2 = fieldReflection.value(object2);
    return value1.toString().compareTo(value2);
  }
}

abstract class Ordering<T> {
  final Type type;
  final String field;

  const Ordering(this.type, this.field);

  int compare(T object1, T object2);
}

const Unordered unordered = const Unordered();

class Unordered extends Ordering {
  const Unordered() : super(null, null);

  int compare(object1, object2) {
    return null;
  }
}

class LikePredicate<T> extends Predicate<T> {
  Type type;
  String field;
  RegExp expression;

  LikePredicate(Type type, String field, String expression) {
    this.type = type;
    this.field = field;
    this.expression = new RegExp(expression.replaceAll(new RegExp(r'%'), '.*'));
  }

  bool evaluate(T object) {
    var value = new TypeReflection(type).fields[field].value(object);
    return expression.hasMatch(value.toString());
  }
}

class EqualsPredicate<T> extends Predicate<T> {
  Type type;
  String field;
  String expression;

  EqualsPredicate(this.type, this.field, this.expression);

  bool evaluate(T object) {
    var value = new TypeReflection(type).fields[field].value(object);
    return expression == value.toString();
  }
}

abstract class Predicate<T> {
  bool evaluate(T object);
}

class Composite {
  Iterable components;

  Composite(this.components);

  int get hashCode => components.map((c) => 11 * c.hashCode).reduce((int c1, int c2) => c1 + 17 * c2);

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



