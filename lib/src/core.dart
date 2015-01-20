part of box;

class Box {
  Box();

  factory Box.file(String filename) => new FileBox(filename);

  Map<String, Map> entities = {
  };

  store(Object entity) {
    String type = new TypeReflection.fromInstance(entity).name;
    entities.putIfAbsent(type, () => new Map());
    entities[type][_keyOf(entity)] = entity;
  }

  _keyOf(Object entity) {
    TypeReflection type = new TypeReflection.fromInstance(entity);
    Iterable key = type.fieldsWith(Key).values.map((field) => field.value(entity));
    return key.length == 1 ? key.first : new Composite(key);
  }

  Future find(Type type, key) async {
    Map entitiesForType = entities[new TypeReflection(type).name];
    return entitiesForType != null ? entitiesForType[key is Iterable ? new Composite(key) : key] : null;
  }
}

class FileBox extends Box {
  String _path;

  FileBox(this._path) {
    installJsonConverters();
  }

  store(Object entity) {
    super.store(entity);
    return new Future(() => _persist(new TypeReflection.fromInstance(entity).name));
  }

  _persist(String type) {
    File file = _fileOf(type);
    if (file.existsSync()) {
      file.deleteSync();
    }
    return file.create(recursive: true).then((file) {
      Json json = Conversion.convert(entities[type].values).to(Json);
      return file.writeAsString(json.toString());
    });
  }

  File _fileOf(String type) {
    return new File(_path + '/' + type);
  }

  Future<List> _load(TypeReflection reflection) {
    File file = _fileOf(reflection.name);
    return file.exists().then((exists) {
      if (exists) {
        return file.readAsString().then((value) {
          return Conversion.convert(new Json(value)).to(List, [reflection.type]);
        });
      } else {
        return new Future.value([]);
      }
    });
  }

  Future find(Type type, key) {
    TypeReflection reflection = new TypeReflection(type);
    String typeName = reflection.name;
    if (!entities.containsKey(typeName)) {
      return _load(reflection).then((values) {
        entities[typeName] = Maps.index(values, (value) => _keyOf(value));
        return super.find(type, key);
      });
    }
    return super.find(type, key);
  }
}

class Composite {
  Iterable components;

  Composite(this.components);

  int get hashCode => components.reduce((Object c1, Object c2) => 11 * c1.hashCode + 17 * c2.hashCode);

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

abstract class Optional<T> {

  const Optional();

  factory Optional.of(T value) {
    return value == null ? empty : new Present(value);
  }

  T get();

  T orElse(T other);

  T orNull();

  T or(T supplier());

  Optional map(dynamic mapper(T value));

  Optional expand(Optional mapper(T value));

  Optional<T> where(bool predicate(T value));

  List<T> toList();

  Optional<T> ifPresent(void handler(T value));

  Optional<T> ifAbsent(void handler());

  bool isPresent();
}

const empty = const Empty();

class Empty<T> extends Optional<T> {
  const Empty();

  T get() => throw new AbsentException();

  T orElse(T other) => other;

  T orNull() => null;

  T or(T supplier()) => supplier();

  Optional map(dynamic mapper(T value)) => this;

  Optional expand(Optional mapper(T value)) => this;

  Optional<T> where(bool predicate(T value)) => this;

  List<T> toList() => [];

  Optional<T> ifPresent(void handler(T value)) => this;

  Optional<T> ifAbsent(void handler()) {
    handler();
    return this;
  }

  bool isPresent() => false;
}

class Present<T> extends Optional<T> {
  T value;

  Present(this.value);

  T get() => value;

  T orElse(T other) => value;

  T orNull() => value;

  T or(T supplier()) => value;

  Optional map(dynamic mapper(T value)) => new Optional.of(mapper(value));

  Optional expand(Optional mapper(T value)) => mapper(value);

  Optional<T> where(bool predicate(T value)) => predicate(value) ? this : empty;

  List<T> toList() => [value];

  Optional<T> ifPresent(void handler(T value)) {
    handler(value);
    return this;
  }

  Optional<T> ifAbsent(void handler()) => this;

  bool isPresent() => true;
}

class AbsentException {
}

