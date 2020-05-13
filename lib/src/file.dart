import 'dart:convert';
import 'dart:io';

import 'package:box/core.dart';
import 'package:box/src/memory.dart';
import 'package:reflective/reflective.dart';

class FileBox extends MemoryBox {
  final String _path;
  bool _persisting = false;

  @override
  bool get persistent => true;

  FileBox(this._path);

  Future store(Object entity) {
    super.store(entity);
    return Future(() => _persist(TypeReflection.fromInstance(entity)));
  }

  Future _persist(TypeReflection type) async {
    if (_persisting) {
      // TODO: stage changes
      throw 'Already persisting changes. Please use await box.store(...) to make sure only one change is persisted at a time';
    }
    _persisting = true;
    var entities = await entitiesFor(type);
    var file = _fileOf(type.name);
    if (file.existsSync()) {
      await file.delete();
    }
    await file.create(recursive: true);
    var json = await Stream.fromIterable(entities.values)
        .map((value) => jsonEncode(Conversion.convert(value).to(Map)).toString())
        .join("\n");
    await file.writeAsString(json.toString(), mode: FileMode.append);
    _persisting = false;
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
        return file.openRead().transform(utf8.decoder).transform(const LineSplitter()).map((line) {
          if (line.startsWith("{")) {
            return Conversion.convert(jsonDecode(line)).to(reflection.rawType);
          }
          return null;
        }).where((item) => item != null);
      }
      return Stream.fromIterable([]);
    })).asyncExpand((Stream stream) => stream);
  }

  Future<T> find<T>(key, [Type type]) {
    TypeReflection reflection = TypeReflection<T>(type);
    String typeName = reflection.name;
    if (entities.isEmpty) {
      return _load(reflection).toList().then((values) {
        entities[typeName] = Maps.index(values, (value) => Box.keyOf(value));
        return super.find<T>(key, type);
      });
    }
    return super.find<T>(key, type);
  }

  @override
  Future deleteAll<T>([Type type]) async {
    await super.deleteAll<T>(type);
    var reflection = TypeReflection<T>(type);
    var file = _fileOf(reflection.name);
    if (file.existsSync()) {
      await file.delete();
    }
  }
}
