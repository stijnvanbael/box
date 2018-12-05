part of box;

class FileBox extends Box {
  String _path;

  FileBox(this._path) {
    Converters.add(new ObjectToBoxJson());
    Converters.add(new BoxJsonToObject());
  }

  Future store(Object entity) {
    super.store(entity);
    return new Future(() =>
        _persist(new TypeReflection.fromInstance(entity)));
  }

  Future _persist(TypeReflection type) {
    return _entitiesFor(type).then((entities) {
      File file = _fileOf(type.name);
      if (file.existsSync()) {
        file.deleteSync();
      }
      return file.create(recursive: true)
          .then((file) => file.writeAsString('[\n'))
          .then((file) => new Stream.fromIterable(entities.values)
            .map((value) => Conversion.convert(value).to(BoxJson).toString())
            .join("\n"))
          .then((json) => file.writeAsString(json.toString(), mode: FileMode.append))
          .then((file) => file.writeAsString('\n]', mode: FileMode.append));
    });
  }

  File _fileOf(String type) {
    return new File(_path + '/' + type);
  }

  Future<Map> _entitiesFor(TypeReflection type) {
    _entities.putIfAbsent(type.name, () => new Map());
    if (_entities[type.name].isEmpty) {
      return _load(type).toList().then((values) {
        _entities[type.name] = Maps.index(values, (value) => Box.keyOf(value));
        return _entities[type.name];
      });
    }
    return new Future.value(_entities[type.name]);
  }

  Stream _load(TypeReflection reflection) {
    File file = _fileOf(reflection.name);
    return new Stream.fromFuture(file.exists().then((exists) {
      if (exists) {
        return file.openRead()
            .transform(utf8.decoder)
            .transform(const LineSplitter())
            .map((line) {
          if (line.startsWith("{"))
            return Conversion.convert(new BoxJson(line)).to(reflection.rawType);
          return null;
        })
            .where((item) => item != null);
      }
      return new Stream.fromIterable([]);
    })).asyncExpand((Stream stream) => stream);
  }

  Future find(Type type, key) {
    TypeReflection reflection = new TypeReflection(type);
    String typeName = reflection.name;
    if (_entities.isEmpty) {
      return _load(reflection).toList().then((values) {
        _entities[typeName] = Maps.index(values,
            (value) => Box.keyOf(value));
        return super.find(type, key);
      });
    }
    return super.find(type, key);
  }
}

class ObjectToBoxJson extends ConverterBase<Object, BoxJson> {
  ObjectToBoxJson()
      : super(new TypeReflection(Object), new TypeReflection(BoxJson));

  BoxJson convertTo(Object object, TypeReflection targetReflection) {
    var simplified = _convert(object);
    return new BoxJson(jsonEncode(simplified));
  }

  _convert(object) {
    if (object is DateTime) {
      return object.toString();
    } else if (object == null || object is String || object is num ||
        object is bool) {
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
  BoxJsonToObject()
      : super(new TypeReflection(BoxJson), new TypeReflection(Object));

  Object convertTo(BoxJson json, TypeReflection targetReflection) {
    var decoded = jsonDecode(json.toString());
    return _convert(decoded, targetReflection);
  }

  _convert(object, TypeReflection targetReflection) {
    if (object is Map) {
      if (targetReflection.sameOrSuper(Map)) {
        TypeReflection keyType = targetReflection.typeArguments[0];
        TypeReflection valueType = targetReflection.typeArguments[1];
        Map map = {
        };
        object.keys.forEach((k) {
          var newKey = keyType.sameOrSuper(k) ? k : keyType.construct(
              args: [k]);
          map[newKey] = _convert(object[k], valueType);
        });
        return map;
      } else {
        var instance = targetReflection.construct();
        object.keys.forEach((k) {
          if (targetReflection.fields[k] == null)
            throw new JsonException(
                'Unknown property: ' + targetReflection.name + '.' + k);
        });
        Maps.forEach(targetReflection.fields,
            (name, field) =>
            field.set(instance, _convert(object[name], field.type)));
        return instance;
      }
    } else if (object is Iterable) {
      TypeReflection itemType = targetReflection.typeArguments[0];
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