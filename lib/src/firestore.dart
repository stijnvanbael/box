library box.firestore;

import 'dart:convert';
import 'dart:io';

import 'package:googleapis_auth/auth_io.dart';
import 'package:googleapis_auth/googleapis_auth.dart';

import '../core.dart';

class FirestoreBox extends Box {
  final String accountKeyFile;
  final String version;
  _Connection? _connection;

  FirestoreBox(this.accountKeyFile, Registry registry, {this.version = 'v1'})
      : super(registry);

  @override
  Future close() async => _connection?.close();

  @override
  Future deleteAll<T>([Type? type]) async {
    var connection = await _connect();
    var documentType = registry.lookup(type ?? T).name;
    try {
      var documents = await connection.list(documentType);
      for (var document in documents) {
        await connection.delete(documentType, document.key);
      }
    } catch (e) {
      print(e); // TODO: find out why this happens
    }
  }

  @override
  Future<T?> find<T>(dynamic key, [Type? type]) async {
    _verifyNoCompositeKey(key);
    var connection = await _connect();
    var entitySupport = registry.lookup(type ?? T);
    var documentType = entitySupport.name;
    var document = await connection.get(documentType, key);
    return document != null ? entitySupport.deserialize(document) : null;
  }

  void _verifyNoCompositeKey(key) {
    if (!(key is String || key is num || key is DateTime)) {
      throw UnimplementedError('Composite keys are not supported');
    }
  }

  @override
  SelectStep select(List<Field> fields) => _SelectStep(this, fields);

  @override
  QueryStep<T> selectFrom<T>([Type? type, String? alias]) {
    return _QueryStep(this, type ?? T, []);
  }

  @override
  Future<K> store<K>(dynamic entity) async {
    var connection = await _connect();
    var entitySupport = registry.lookup(entity.runtimeType);
    var documentType = entitySupport.name;
    var key = entitySupport.getKey(entity);
    _verifyNoCompositeKey(key);
    var document = entitySupport.serialize(entity);
    await connection.patch(documentType, key, document);
    return key as K;
  }

  Future<_Connection> _connect() async {
    if (_connection == null) {
      var keyFile = File(accountKeyFile);
      var keys = jsonDecode(await keyFile.readAsString());
      var credentials = ServiceAccountCredentials.fromJson(keys);
      var client = await clientViaServiceAccount(
          credentials, ['https://www.googleapis.com/auth/datastore']);
      _connection = _Connection(client, keys['project_id'], version);
    }
    return _connection!;
  }

  @override
  bool get supportsCompositeKey => false;

  @override
  bool get supportsLike => false;

  @override
  bool get supportsNot => false;

  @override
  bool get supportsOr => false;

  @override
  bool get supportsIn => false;

  @override
  DeleteStep<T> deleteFrom<T>([Type? type]) {
    // TODO: implement deleteFrom
    throw UnimplementedError();
  }
}

class _SelectStep implements SelectStep {
  final FirestoreBox _box;
  final List<Field> _fields;

  _SelectStep(this._box, this._fields);

  @override
  _QueryStep from(Type type, [String? alias]) =>
      _QueryStep(_box, type, _fields);
}

class _QueryStep<T> extends _ExpectationStep<T> implements QueryStep<T> {
  _QueryStep(FirestoreBox box, Type type, List<Field> fields)
      : super(box, null, null, type, fields);

  _QueryStep.withFilter(_QueryStep<T> query, Map<String, dynamic> filter)
      : super(
            query.box, filter, query._order, query._type, query._selectFields);

  @override
  WhereStep<T, QueryStep<T>> and(String field) => _AndStep(field, this);

  @override
  WhereStep<T, QueryStep<T>> or(String field) => _OrStep(field, this);

  @override
  OrderByStep<T> orderBy(String field) => _OrderByStep(field, this);

  @override
  WhereStep<T, QueryStep<T>> where(String field) =>
      _QueryWhereStep(field, this);

  @override
  JoinStep<T> innerJoin(Type type, [String? alias]) {
    // TODO: implement innerJoin
    throw UnimplementedError();
  }
}

class _QueryWhereStep<T> implements WhereStep<T, QueryStep<T>> {
  final String field;
  final _QueryStep<T> query;

  _QueryWhereStep(this.field, this.query);

  QueryStep<T> _queryStep(Map<String, dynamic> filter) =>
      _QueryStep<T>.withFilter(query, combine(filter));

  Map<String, dynamic> combine(Map<String, dynamic> selector) => selector;

  @override
  QueryStep<T> between(dynamic value1, dynamic value2) =>
      gt(value1).and(field).lt(value2);

  @override
  QueryStep<T> equals(dynamic value) => _fieldFilter('EQUAL', value);

  @override
  QueryStep<T> gt(dynamic value) => _fieldFilter('GREATER_THAN', value);

  @override
  QueryStep<T> gte(dynamic value) =>
      _fieldFilter('GREATER_THAN_OR_EQUAL', value);

  @override
  QueryStep<T> like(String expression) {
    throw UnimplementedError('Like is not supported');
  }

  @override
  QueryStep<T> lt(dynamic value) => _fieldFilter('LESS_THAN', value);

  @override
  QueryStep<T> lte(dynamic value) => _fieldFilter('LESS_THAN_OR_EQUAL', value);

  @override
  QueryStep<T> in_(Iterable<dynamic> values) {
    throw UnimplementedError('OneOf is not supported');
  }

  @override
  WhereStep<T, QueryStep<T>> not() {
    throw UnimplementedError('Not is not supported');
  }

  @override
  QueryStep<T> contains(value) => _fieldFilter('ARRAY_CONTAINS', value);

  QueryStep<T> _fieldFilter(String operator, dynamic value) => _queryStep({
        'fieldFilter': {
          'field': {'fieldPath': field},
          'op': operator,
          'value': _wrap(value),
        },
      });
}

class _OrStep<T> extends _QueryWhereStep<T> {
  _OrStep(String field, _QueryStep<T> query) : super(field, query);

  @override
  Map<String, dynamic> combine(Map<String, dynamic> filter) =>
      query._filter != null
          ? {
              'compositeFilter': {
                'op': 'OR',
                'filters': [query._filter, filter]
              }
            }
          : filter;
}

class _AndStep<T> extends _QueryWhereStep<T> {
  _AndStep(String field, _QueryStep<T> query) : super(field, query);

  @override
  Map<String, dynamic> combine(Map<String, dynamic> filter) =>
      query._filter != null
          ? {
              'compositeFilter': {
                'op': 'AND',
                'filters': [query._filter, filter]
              }
            }
          : filter;
}

class _OrderByStep<T> implements OrderByStep<T> {
  final String field;
  final _QueryStep<T> _query;

  _OrderByStep(this.field, this._query);

  @override
  ExpectationStep<T> ascending() => _orderBy('ASCENDING');

  @override
  ExpectationStep<T> descending() => _orderBy('DESCENDING');

  ExpectationStep<T> _orderBy(String direction) => _ExpectationStep(
      _query.box,
      _query._filter,
      [
        {
          'field': {'fieldPath': field},
          'direction': direction,
        }
      ],
      _query._type,
      _query._selectFields);
}

class _ExpectationStep<T> extends ExpectationStep<T> {
  @override
  final FirestoreBox box;
  final Map<String, dynamic>? _filter;
  final List<Map<String, dynamic>>? _order;
  final Type _type;
  final List<Field> _selectFields;

  _ExpectationStep(
      this.box, this._filter, this._order, this._type, this._selectFields);

  @override
  Stream<T> stream({int limit = 1000000, int offset = 0}) async* {
    var entitySupport = box.registry.lookup(_type);
    var connection = await box._connect();
    var documents = (await connection.query(
        _selectFields, entitySupport.name, _filter, _order, limit, offset));
    for (var document in documents) {
      if (_selectFields.isEmpty) {
        yield entitySupport.deserialize(document) as T;
      } else {
        yield _applyFieldAliases(document) as T;
      }
    }
  }

  Map<String, dynamic> _applyFieldAliases(Map<String, dynamic> document) {
    var result = <String, dynamic>{};
    _selectFields.forEach((field) {
      result[field.alias] = _getValue(document, field.name);
    });
    return result;
  }

  dynamic _getValue(Map<String, dynamic> document, String name) =>
      name.contains('.')
          ? _getValue(_getValue(document, name.substring(0, name.indexOf('.'))),
              name.substring(name.indexOf('.') + 1))
          : document[name];
}

class _Connection {
  final AuthClient _client;
  final String _projectId;
  final String _version;

  _Connection(this._client, this._projectId, this._version);

  Future<List<_Document>> list(String documentType) async {
    var response =
        await _client.get(Uri.parse('$_urlPrefix/documents/$documentType'));
    if (response.statusCode >= 400) {
      throw 'Error deleting $documentType $key: ${response.statusCode}\n${response.body}';
    }
    var documents = jsonDecode(response.body)['documents'] ?? [];
    return List<_Document>.from(
        documents.map((map) => _Document.fromJson(map)));
  }

  Future delete(String documentType, String key) async {
    var response = await _client
        .delete(Uri.parse('$_urlPrefix/documents/$documentType/$key'));
    if (response.statusCode >= 400) {
      throw 'Error deleting $documentType $key: ${response.statusCode}\n${response.body}';
    }
  }

  Future patch(
      String documentType, String key, Map<String, dynamic> document) async {
    var response = await _client.patch(
        Uri.parse('$_urlPrefix/documents/$documentType/$key'),
        body: jsonEncode(_wrap(document)['mapValue']));
    if (response.statusCode >= 400) {
      throw 'Error patching $documentType $key: ${response.statusCode}\n${response.body}';
    }
  }

  String get _urlPrefix =>
      'https://firestore.googleapis.com/$_version/projects/$_projectId/databases/(default)';

  void close() => _client.close();

  Future<Map<String, dynamic>?> get(String documentType, String key) async {
    var response = await _client
        .get(Uri.parse('$_urlPrefix/documents/$documentType/$key'));
    if (response.statusCode == 404) {
      return null;
    }
    if (response.statusCode >= 400) {
      throw 'Error getting $documentType $key: ${response.statusCode}\n${response.body}';
    }
    return _unwrap({'mapValue': jsonDecode(response.body)});
  }

  Future<List<Map<String, dynamic>>> query(
      List<Field> selectFields,
      String documentType,
      Map<String, dynamic>? filter,
      List<Map<String, dynamic>>? order,
      int limit,
      int offset) async {
    var query = {
      'from': [
        {'collectionId': documentType}
      ],
      'where': filter,
      'orderBy': order,
      'limit': limit,
      'offset': offset,
    };
    if (selectFields.isNotEmpty) {
      query['select'] = {
        'fields':
            selectFields.map((field) => {'fieldPath': field.name}).toList()
      };
    }
    var response = await _client.post(
      Uri.parse('$_urlPrefix/documents:runQuery'),
      body: jsonEncode({'structuredQuery': query}),
    );
    if (response.statusCode >= 400) {
      throw 'Error querying $documentType $key: ${response.statusCode}\n${response.body}';
    }
    var documents = jsonDecode(response.body);
    return List<Map<String, dynamic>>.from(documents
        .map((map) => map.containsKey('document')
            ? _unwrap({'mapValue': map['document']})
            : null)
        .where((element) => element != null));
  }
}

dynamic _wrap(dynamic object) {
  if (object is String) {
    return {'stringValue': object};
  } else if (object == null) {
    return {'nullValue': null};
  } else if (object is Map) {
    return {
      'mapValue': {
        'fields': object.map((key, value) => MapEntry(key, _wrap(value)))
      }
    };
  } else if (object is List) {
    return {
      'arrayValue': {'values': object.map((value) => _wrap(value)).toList()}
    };
  } else {
    return object;
  }
}

dynamic _unwrap(dynamic object) {
  if (object is Map) {
    if (object.containsKey('stringValue')) {
      return object['stringValue'];
    } else if (object.containsKey('nullValue')) {
      return null;
    } else if (object.containsKey('mapValue')) {
      return Map<String, dynamic>.from(object['mapValue']['fields']
          .map((key, value) => MapEntry(key, _unwrap(value))));
    } else if (object.containsKey('values')) {
      return List.from(
          object['arrayValue']['values'].map((value) => _unwrap(value)));
    }
  } else {
    return object;
  }
}

class _Document {
  final String name;
  final DateTime createTime;
  final DateTime updateTime;

  _Document({
    required this.name,
    required this.createTime,
    required this.updateTime,
  });

  _Document.fromJson(Map map)
      : this(
          name: map['name'],
          createTime: DateTime.parse(map['createTime']),
          updateTime: DateTime.parse(map['updateTime']),
        );

  String get key => name.substring(name.lastIndexOf('/') + 1);
}
