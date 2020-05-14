library box.file;

import 'dart:convert';
import 'dart:io';

import 'package:box/core.dart';
import 'package:box/src/memory.dart';

class FileBox extends MemoryBox {
  final String _path;
  bool _persisting = false;

  @override
  bool get persistent => true;

  FileBox(this._path, Registry registry) : super(registry);

  @override
  Future store(Object entity) {
    super.store(entity);
    return Future(() => _persist(entity.runtimeType));
  }

  Future _persist(Type type) async {
    if (_persisting) {
      // TODO: stage changes
      throw 'Already persisting changes. Please use await box.store(...) to make sure only one change is persisted at a time';
    }
    _persisting = true;
    var entities = await entitiesFor(type);
    var file = _fileOf(registry.lookup(type).name);
    if (file.existsSync()) {
      await file.delete();
    }
    await file.create(recursive: true);
    var json = await Stream.fromIterable(entities.values).map((value) => jsonEncode(value)).join('\n');
    await file.writeAsString(json.toString(), mode: FileMode.append);
    _persisting = false;
  }

  File _fileOf(String type) {
    return File(_path + '/' + type);
  }

  @override
  Future<Map> entitiesFor(Type type) {
    var typeName = registry.lookup(type).name;
    entities.putIfAbsent(typeName, () => {});
    if (entities[typeName].isEmpty) {
      return _load(type).toList().then((values) {
        entities[typeName] = {for (var value in values) keyOf(value): value};
        return entities[typeName];
      });
    }
    return Future.value(entities[typeName]);
  }

  Stream _load(Type type) {
    var entitySupport = registry.lookup(type);
    var file = _fileOf(entitySupport.name);
    return Stream.fromFuture(file.exists().then((exists) {
      if (exists) {
        return file.openRead().transform(utf8.decoder).transform(const LineSplitter()).map((line) {
          if (line.startsWith('{')) {
            return entitySupport.deserialize(jsonDecode(line));
          }
          return null;
        }).where((item) => item != null);
      }
      return Stream.fromIterable([]);
    })).asyncExpand((Stream stream) => stream);
  }

  @override
  Future<T> find<T>(key, [Type type]) {
    var typeName = registry.lookup(type ?? T).name;
    if (entities.isEmpty) {
      return _load(type ?? T).toList().then((values) {
        entities[typeName] = {for (var value in values) keyOf(value): value};
        return super.find<T>(key, type ?? T);
      });
    }
    return super.find<T>(key, type ?? T);
  }

  @override
  Future deleteAll<T>([Type type]) async {
    await super.deleteAll<T>(type);
    var file = _fileOf(registry.lookup(type ?? T).name);
    if (file.existsSync()) {
      await file.delete();
    }
  }
}
