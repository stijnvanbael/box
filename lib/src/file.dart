import 'dart:convert';
import 'dart:io';

import 'package:box/core.dart';
import 'package:box/src/memory.dart';
import 'package:reflective/reflective.dart';

class FileBox extends MemoryBox {
  final String _path;

  FileBox(this._path) {
    Converters.add(_ObjectToBoxJson());
    Converters.add(_BoxJsonToObject());
  }

  Future store(Object entity) {
    super.store(entity);
    return Future(() => _persist(TypeReflection.fromInstance(entity)));
  }

  Future _persist(TypeReflection type) {
    return entitiesFor(type).then((entities) {
      File file = _fileOf(type.name);
      if (file.existsSync()) {
        file.deleteSync();
      }
      return file
          .create(recursive: true)
          .then((file) => Stream.fromIterable(entities.values)
              .map((value) => Conversion.convert(value).to(_BoxJson).toString())
              .join("\n"))
          .then((json) =>
              file.writeAsString(json.toString(), mode: FileMode.append));
    });
  }

  File _fileOf(String type) {
    return File(_path + '/' + type);
  }

  @override
  Future<Map> entitiesFor(TypeReflection type) {
    entities.putIfAbsent(type.name, () => Map());
    if (entities[type.name].isEmpty) {
      return _load(type).toList().then((values) {
        entities[type.name] = Maps.index(values, (value) => Box.keyOf(value));
        return entities[type.name];
      });
    }
    return Future.value(entities[type.name]);
  }

  Stream _load(TypeReflection reflection) {
    File file = _fileOf(reflection.name);
    return Stream.fromFuture(file.exists().then((exists) {
      if (exists) {
        return file
            .openRead()
            .cast<List<int>>()
            .transform(utf8.decoder)
            .transform(const LineSplitter())
            .map((line) {
          if (line.startsWith("{"))
            return Conversion.convert(_BoxJson(line)).to(reflection.rawType);
          return null;
        }).where((item) => item != null);
      }
      return Stream.fromIterable([]);
    })).asyncExpand((Stream stream) => stream);
  }

  Future<T> find<T>(key) {
    TypeReflection reflection = TypeReflection<T>();
    String typeName = reflection.name;
    if (entities.isEmpty) {
      return _load(reflection).toList().then((values) {
        entities[typeName] = Maps.index(values, (value) => Box.keyOf(value));
        return super.find<T>(key);
      });
    }
    return super.find<T>(key);
  }
}

class _ObjectToBoxJson extends ConverterBase<Object, _BoxJson> {
  _ObjectToBoxJson() : super(TypeReflection(Object), TypeReflection(_BoxJson));

  _BoxJson convertTo(Object object, TypeReflection targetReflection) {
    var simplified = _convert(object);
    return _BoxJson(jsonEncode(simplified));
  }

  _convert(object) {
    if (object is DateTime) {
      return object.toString();
    } else if (object == null ||
        object is String ||
        object is num ||
        object is bool) {
      return object;
    } else if (object is Iterable) {
      return List.from(object.map((item) => _convert(item)));
    } else if (object is Map) {
      Map map = {};
      object.keys.forEach((k) => map[k.toString()] = _convert(object[k]));
      return map;
    } else {
      TypeReflection type = TypeReflection.fromInstance(object);
      return type.fields.values
          .where((field) => !field.has(Transient))
          .map((field) => {field.name: _convert(field.value(object))})
          .reduce((Map m1, Map m2) {
        m2.addAll(m1);
        return m2;
      });
    }
  }
}

class _BoxJsonToObject extends ConverterBase<_BoxJson, Object> {
  _BoxJsonToObject() : super(TypeReflection(_BoxJson), TypeReflection(Object));

  Object convertTo(_BoxJson json, TypeReflection targetReflection) {
    var decoded = jsonDecode(json.toString());
    return _convert(decoded, targetReflection);
  }

  _convert(object, TypeReflection targetReflection) {
    if (object is Map) {
      if (targetReflection.sameOrSuper(Map)) {
        TypeReflection keyType = targetReflection.typeArguments[0];
        TypeReflection valueType = targetReflection.typeArguments[1];
        Map map = {};
        object.keys.forEach((k) {
          var newKey =
              keyType.sameOrSuper(k) ? k : keyType.construct(args: [k]);
          map[newKey] = _convert(object[k], valueType);
        });
        return map;
      } else {
        var instance = targetReflection.construct();
        object.keys.forEach((k) {
          if (targetReflection.fields[k] == null)
            throw JsonException(
                'Unknown property: ' + targetReflection.name + '.' + k);
        });
        Maps.forEach(
            targetReflection.fields,
            (name, field) =>
                field.set(instance, _convert(object[name], field.type)));
        return instance;
      }
    } else if (object is Iterable) {
      TypeReflection itemType = targetReflection.typeArguments[0];
      return List.from(object.map((i) => _convert(i, itemType)));
    } else if (targetReflection.sameOrSuper(DateTime)) {
      return DateTime.parse(object);
    } else {
      return object;
    }
  }
}

class _BoxJson extends Json {
  _BoxJson(String json) : super(json);
}
