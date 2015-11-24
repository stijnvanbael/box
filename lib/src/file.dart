part of box;

class FileBox extends Box {
  String _path;

  FileBox(this._path) {
    Converters.add(new ObjectToBoxJson());
    Converters.add(new BoxJsonToObject());
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
      BoxJson json = Conversion.convert(_entitiesFor(type).values).to(BoxJson);
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
          return Conversion.convert(new BoxJson(value)).to(List, [reflection.type]);
        });
      } else {
        return new Future.value([]);
      }
    });
  }

  Future find(Type type, key) {
    TypeReflection reflection = new TypeReflection(type);
    String typeName = reflection.name;
    if (!_entities.containsKey(typeName)) {
      return _load(reflection).then((values) {
        _entities[typeName] = Maps.index(values, (value) => Box.keyOf(value));
        return super.find(type, key);
      });
    }
    return super.find(type, key);
  }
}

class ObjectToBoxJson extends ConverterBase<Object, BoxJson> {
  ObjectToBoxJson() : super(new TypeReflection(Object), new TypeReflection(BoxJson));

  BoxJson convertTo(Object object, TypeReflection targetReflection) {
    var simplified = _convert(object);
    return new BoxJson(JSON.encode(simplified));
  }

  _convert(object) {
    if (object is DateTime) {
      return object.toString();
    } else if (object == null || object is String || object is num || object is bool) {
      return object;
    } else if (object is Iterable) {
      return new List.from(object.map((item) => _convert(item)));
    } else if (object is Map) {
      Map map = {
      };
      object.keys.forEach((k) => map[k.toString()] = _convert(object[k]));
      return map;
    } else {
      TypeReflection type = new TypeReflection.fromInstance(object);
      return type.fields.values
      .where((field) => !field.has(Transient))
      .map((field) => {
          field.name: _convert(field.value(object))
      })
      .reduce((Map m1, Map m2) {
        m2.addAll(m1);
        return m2;
      });
    }
  }
}

class BoxJsonToObject extends ConverterBase<BoxJson, Object> {
  BoxJsonToObject() : super(new TypeReflection(BoxJson), new TypeReflection(Object));

  Object convertTo(BoxJson json, TypeReflection targetReflection) {
    var decoded = JSON.decode(json.toString());
    return _convert(decoded, targetReflection);
  }

  _convert(object, TypeReflection targetReflection) {
    if (object is Map) {
      if (targetReflection.sameOrSuper(Map)) {
        TypeReflection keyType = targetReflection.arguments[0];
        TypeReflection valueType = targetReflection.arguments[1];
        Map map = {
        };
        object.keys.forEach((k) {
          var newKey = keyType.sameOrSuper(k) ? k : keyType.construct(args: [k]);
          map[newKey] = _convert(object[k], valueType);
        });
        return map;
      } else {
        var instance = targetReflection.construct();
        object.keys.forEach((k) {
          if (targetReflection.fields[k] == null)
            throw new JsonException('Unknown property: ' + targetReflection.name + '.' + k);
        });
        Maps.forEach(targetReflection.fields,
            (name, field) => field.set(instance, _convert(object[name], field.type)));
        return instance;
      }
    } else if (object is Iterable) {
      TypeReflection itemType = targetReflection.arguments[0];
      return new List.from(object.map((i) => _convert(i, itemType)));
    } else if (targetReflection.sameOrSuper(DateTime)) {
      return DateTime.parse(object);
    } else {
      return object;
    }
  }
}

class BoxJson extends Json {
  BoxJson(String json) : super(json);
}