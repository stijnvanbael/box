import 'package:box/box.dart';
import 'package:box/firestore.dart';
import 'package:box/mongodb.dart';
import 'package:collection/collection.dart';
import 'package:test/test.dart';

part 'box.g.dart';

var registry = initBoxRegistry();
var firestore = FirestoreBox('.secrets/firestore.json', registry);

void main() async {
  await runTests('Memory', () => MemoryBox(registry));
  await runTests('File', () => FileBox('.box/test', registry));
  await runTests('MongoDB', () => MongoDbBox('localhost', registry, database: 'test'));
  await runTests('Firestore', () => firestore);
}

void runTests(String name, Box Function() boxBuilder) async {
  Future<Box> reconnectIfPersistent(Box box) async {
    if (box != firestore && box.persistent) {
      await box.close();
      return boxBuilder();
    }
    return box;
  }

  var john = User(id: 'jdoe', name: 'John Doe');

  Future<Box> setUp() async {
    var box = boxBuilder();
    await box.deleteAll<User>();
    await box.deleteAll<Post>();
    return box;
  }

  group('$name - Find by key', () {
    test('Find by single key', () async {
      var box = await setUp();
      expect(await box.find<User>('jdoe'), isNull);

      var user = john;
      await box.store(user);

      box = await reconnectIfPersistent(box);
      expect(await box.find<User>('jdoe'), equals(user));
    });

    test('Find by composite key', () async {
      var box = await setUp();
      await box.deleteAll<Post>();
      var user = john;
      var timestamp = DateTime.parse('2014-12-11T10:09:08Z');
      expect(await box.find<Post>({'userId': user.id, 'timestamp': timestamp}), isNull);

      var post = Post(
          userId: user.id,
          timestamp: timestamp,
          text: 'I just discovered dart-box\nIt\'s awesome!',
          keywords: ['persistence', 'dart']);
      await box.store(post);

      box = await reconnectIfPersistent(box);
      var found = await box.find<Post>({'userId': user.id, 'timestamp': timestamp});
      expect(found, equals(post));
    }, skip: !boxBuilder().compositeKeySupported);
  });

  group('$name - Predicates', () {
    test('equals predicate, unique', () async {
      var jdoe = john;
      var crollis = User(id: 'crollis', name: 'Christine Rollis');
      var cstone = User(id: 'cstone', name: 'Cora Stone');
      var dsnow = User(id: 'dsnow', name: 'Donovan Snow');
      var koneil = User(id: 'koneil', name: 'Kendall Oneil');
      var box = await setUp();
      await box.storeAll([jdoe, crollis, cstone, dsnow, koneil]);

      box = await reconnectIfPersistent(box);
      expect((await box.selectFrom<User>().where('name').equals('Cora Stone').unique()), equals(cstone));
    });

    test('like predicate, list, order by', () async {
      var jdoe = john;
      var crollis = User(id: 'crollis', name: 'Christine Rollis');
      var cstone = User(id: 'cstone', name: 'Cora Stone');
      var dsnow = User(id: 'dsnow', name: 'Donovan Snow');
      var koneil = User(id: 'koneil', name: 'Kendall Oneil');
      var box = await setUp();
      await box.storeAll([jdoe, crollis, cstone, dsnow, koneil]);

      box = await reconnectIfPersistent(box);
      expect(await box.selectFrom<User>().where('name').like('C%').orderBy('name').ascending().list(),
          equals([crollis, cstone]));
    }, skip: !boxBuilder().likeSupported);

    test('gt predicate', () async {
      var jdoe = john;
      var crollis = User(id: 'crollis', name: 'Christine Rollis');
      var cstone = User(id: 'cstone', name: 'Cora Stone');
      var dsnow = User(id: 'dsnow', name: 'Donovan Snow');
      var koneil = User(id: 'koneil', name: 'Kendall Oneil');
      var box = await setUp();
      await box.storeAll([jdoe, crollis, cstone, dsnow, koneil]);

      box = await reconnectIfPersistent(box);
      expect((await box.selectFrom<User>().where('name').gt('Cora Stone').orderBy('name').ascending().list()),
          equals([dsnow, jdoe, koneil]));
    });

    test('gte predicate', () async {
      var jdoe = john;
      var crollis = User(id: 'crollis', name: 'Christine Rollis');
      var cstone = User(id: 'cstone', name: 'Cora Stone');
      var dsnow = User(id: 'dsnow', name: 'Donovan Snow');
      var koneil = User(id: 'koneil', name: 'Kendall Oneil');
      var box = await setUp();
      await box.storeAll([jdoe, crollis, cstone, dsnow, koneil]);

      box = await reconnectIfPersistent(box);
      expect((await box.selectFrom<User>().where('name').gte('Cora Stone').orderBy('name').ascending().list()),
          equals([cstone, dsnow, jdoe, koneil]));
    });

    test('lt predicate', () async {
      var jdoe = john;
      var crollis = User(id: 'crollis', name: 'Christine Rollis');
      var cstone = User(id: 'cstone', name: 'Cora Stone');
      var dsnow = User(id: 'dsnow', name: 'Donovan Snow');
      var koneil = User(id: 'koneil', name: 'Kendall Oneil');
      var box = await setUp();
      await box.storeAll([jdoe, crollis, cstone, dsnow, koneil]);

      box = await reconnectIfPersistent(box);
      expect((await box.selectFrom<User>().where('name').lt('Donovan Snow').orderBy('name').ascending().list()),
          equals([crollis, cstone]));
    });

    test('lte predicate', () async {
      var jdoe = john;
      var crollis = User(id: 'crollis', name: 'Christine Rollis');
      var cstone = User(id: 'cstone', name: 'Cora Stone');
      var dsnow = User(id: 'dsnow', name: 'Donovan Snow');
      var koneil = User(id: 'koneil', name: 'Kendall Oneil');
      var box = await setUp();
      await box.storeAll([jdoe, crollis, cstone, dsnow, koneil]);

      box = await reconnectIfPersistent(box);
      expect((await box.selectFrom<User>().where('name').lte('Donovan Snow').orderBy('name').ascending().list()),
          equals([crollis, cstone, dsnow]));
    });

    test('between predicate', () async {
      var jdoe = john;
      var crollis = User(id: 'crollis', name: 'Christine Rollis');
      var cstone = User(id: 'cstone', name: 'Cora Stone');
      var dsnow = User(id: 'dsnow', name: 'Donovan Snow');
      var koneil = User(id: 'koneil', name: 'Kendall Oneil');
      var box = await setUp();
      await box.storeAll([jdoe, crollis, cstone, dsnow, koneil]);

      box = await reconnectIfPersistent(box);
      expect((await box.selectFrom<User>().where('name').between('Ci', 'E').orderBy('name').ascending().list()),
          equals([cstone, dsnow]));
    });
  });

  group('$name - Operators', () {
    test('AND', () async {
      var jdoe = john;
      var crollis = User(id: 'crollis', name: 'Christine Rollis');
      var cstone = User(id: 'cstone', name: 'Cora Stone');
      var dsnow = User(id: 'dsnow', name: 'Donovan Snow');
      var koneil = User(id: 'koneil', name: 'Kendall Oneil');
      var box = await setUp();
      await box.storeAll([jdoe, crollis, cstone, dsnow, koneil]);

      box = await reconnectIfPersistent(box);
      expect(await box.selectFrom<User>().where('id').equals('cstone').and('name').equals('Cora Stone').list(),
          equals([cstone]));
    });

    test('OR', () async {
      var jdoe = john;
      var crollis = User(id: 'crollis', name: 'Christine Rollis');
      var cstone = User(id: 'cstone', name: 'Cora Stone');
      var dsnow = User(id: 'dsnow', name: 'Donovan Snow');
      var koneil = User(id: 'koneil', name: 'Kendall Oneil');
      var box = await setUp();
      await box.storeAll([jdoe, crollis, cstone, dsnow, koneil]);

      box = await reconnectIfPersistent(box);
      expect(
          await box
              .selectFrom<User>()
              .where('name')
              .equals('Cora Stone')
              .or('name')
              .equals('Donovan Snow')
              .orderBy('name')
              .ascending()
              .list(),
          equals([cstone, dsnow]));
    }, skip: !boxBuilder().orSupported);

    test('NOT, descending', () async {
      var jdoe = john;
      var crollis = User(id: 'crollis', name: 'Christine Rollis');
      var cstone = User(id: 'cstone', name: 'Cora Stone');
      var dsnow = User(id: 'dsnow', name: 'Donovan Snow');
      var koneil = User(id: 'koneil', name: 'Kendall Oneil');
      var box = await setUp();
      await box.storeAll([jdoe, crollis, cstone, dsnow, koneil]);

      box = await reconnectIfPersistent(box);
      expect(
          await box.selectFrom<User>().where('name').not().equals('Donovan Snow').orderBy('name').descending().list(),
          equals([koneil, jdoe, cstone, crollis]));
    }, skip: !boxBuilder().notSupported);
  });

  group('$name - Deep queries', () {
    test('Deep query into value object', () async {
      var crollis =
          User(id: 'crollis', name: 'Christine Rollis', lastPost: Post(text: 'Dart 2.6.1 is out!', keywords: ['dart']));
      var cstone = User(
          id: 'cstone',
          name: 'Cora Stone',
          lastPost: Post(text: 'Cupcakes are ready!', keywords: ['baking', 'cupcakes']));
      var dsnow = User(
          id: 'dsnow',
          name: 'Donovan Snow',
          lastPost: Post(text: 'I just discovered dart-box\nIt\'s awesome!', keywords: ['persistence', 'dart']));
      var koneil = User(
          id: 'koneil',
          name: 'Kendall Oneil',
          lastPost: Post(text: 'Has anyone seen my dog?', keywords: ['dog', 'lost']));
      var box = await setUp();
      await box.storeAll([crollis, cstone, dsnow, koneil]);

      box = await reconnectIfPersistent(box);
      expect(
          await box.selectFrom<User>().where('lastPost.text').equals('Dart 2.6.1 is out!').list(), equals([crollis]));
    });
  });

  group('$name - Limit and offset', () {
    test('Limit', () async {
      var jdoe = john;
      var crollis = User(id: 'crollis', name: 'Christine Rollis');
      var cstone = User(id: 'cstone', name: 'Cora Stone');
      var dsnow = User(id: 'dsnow', name: 'Donovan Snow');
      var koneil = User(id: 'koneil', name: 'Kendall Oneil');
      var box = await setUp();
      await box.storeAll([jdoe, crollis, cstone, dsnow, koneil]);

      box = await reconnectIfPersistent(box);
      expect(await box.selectFrom<User>().orderBy('name').ascending().list(limit: 3), equals([crollis, cstone, dsnow]));
    });
    test('Offset', () async {
      var jdoe = john;
      var crollis = User(id: 'crollis', name: 'Christine Rollis');
      var cstone = User(id: 'cstone', name: 'Cora Stone');
      var dsnow = User(id: 'dsnow', name: 'Donovan Snow');
      var koneil = User(id: 'koneil', name: 'Kendall Oneil');
      var box = await setUp();
      await box.storeAll([jdoe, crollis, cstone, dsnow, koneil]);

      box = await reconnectIfPersistent(box);
      expect(await box.selectFrom<User>().orderBy('name').ascending().list(offset: 2), equals([dsnow, jdoe, koneil]));
    });
  });

  group('$name - Dynamic type parameters', () {
    test('select', () async {
      var crollis = User(id: 'crollis', name: 'Christine Rollis');
      var cstone = User(id: 'cstone', name: 'Cora Stone');
      var dsnow = User(id: 'dsnow', name: 'Donovan Snow');
      var box = await setUp();
      await box.storeAll([crollis, cstone, dsnow]);

      box = await reconnectIfPersistent(box);
      expect(await box.selectFrom(User).orderBy('name').ascending().list(), equals([crollis, cstone, dsnow]));
    });

    test('find', () async {
      var crollis = User(id: 'crollis', name: 'Christine Rollis');
      var cstone = User(id: 'cstone', name: 'Cora Stone');
      var dsnow = User(id: 'dsnow', name: 'Donovan Snow');
      var box = await setUp();
      await box.storeAll([crollis, cstone, dsnow]);

      box = await reconnectIfPersistent(box);
      expect(await box.find('dsnow', User), equals(dsnow));
    });
  });

  group('$name - Select fields', () {
    test('As simple map', () async {
      var crollis = User(id: 'crollis', name: 'Christine Rollis', lastPost: Post(text: 'Bye!'));
      var cstone = User(id: 'cstone', name: 'Cora Stone', lastPost: Post(text: 'Signing off'));
      var dsnow = User(id: 'dsnow', name: 'Donovan Snow', lastPost: Post(text: 'Hi!'));
      var box = await setUp();
      await box.storeAll([crollis, cstone, dsnow]);

      box = await reconnectIfPersistent(box);
      expect(
          await box
              .select([$('name'), $('lastPost.text', alias: 'words')])
              .from(User)
              .where('id')
              .equals('crollis')
              .list(),
          equals([
            {'name': 'Christine Rollis', 'words': 'Bye!'}
          ]));
    });
    test('Convert result', () async {
      var crollis = User(id: 'crollis', name: 'Christine Rollis', lastPost: Post(text: 'Bye!'));
      var cstone = User(id: 'cstone', name: 'Cora Stone', lastPost: Post(text: 'Signing off'));
      var dsnow = User(id: 'dsnow', name: 'Donovan Snow', lastPost: Post(text: 'Hi!'));
      var box = await setUp();
      await box.storeAll([crollis, cstone, dsnow]);

      box = await reconnectIfPersistent(box);
      expect(
          await box
              .select([$('name'), $('lastPost.text', alias: 'words')])
              .from(User)
              .orderBy('name')
              .ascending()
              .mapTo<LastWords>()
              .list(),
          equals([
            LastWords(name: 'Christine Rollis', words: 'Bye!'),
            LastWords(name: 'Cora Stone', words: 'Signing off'),
            LastWords(name: 'Donovan Snow', words: 'Hi!'),
          ]));
    });
  });
}

@entity
class LastWords {
  String name;
  String words;

  LastWords({this.name, this.words});

  LastWords.fromJson(Map map) : this(name: map['name'], words: map['words']);

  @override
  String toString() => '$name: $words';

  @override
  bool operator ==(Object other) =>
      identical(this, other) ||
      other is LastWords && runtimeType == other.runtimeType && name == other.name && words == other.words;

  @override
  int get hashCode => name.hashCode ^ words.hashCode;

  Map<String, dynamic> toJson() => {'name': name, 'words': words};
}

@entity
class User {
  @key
  String id;
  String name;
  Post lastPost;
  List<Post> posts;

  User({this.id, this.name, this.lastPost, this.posts});

  User.fromJson(Map map)
      : this(
          id: map['id'],
          name: map['name'],
          lastPost: map['lastPost'] != null ? Post.fromJson(map['lastPost']) : null,
          posts: map['posts'] != null ? map['posts'].map((postMap) => Post.fromJson(postMap)) : null,
        );

  @override
  String toString() => '@$id ($name)';

  @override
  int get hashCode => id.hashCode ^ name.hashCode;

  @override
  bool operator ==(other) {
    if (other is! User) return false;
    User user = other;
    return (user.id == id && user.name == name);
  }

  Map<String, dynamic> toJson() => {
        'id': id,
        'name': name,
        'lastPost': lastPost?.toJson(),
        'posts': posts?.map((post) => post.toJson()),
      };
}

@entity
class Post {
  @key
  String userId;
  @Key()
  DateTime timestamp;
  String text;
  List<String> keywords;

  Post({this.userId, this.timestamp, this.text, this.keywords});

  Post.fromJson(Map map)
      : this(
          userId: map['userId'],
          timestamp: map['timestamp'] != null ? DateTime.parse(map['timestamp']) : null,
          text: map['text'],
          keywords: map['keywords'] != null ? List<String>.from(map['keywords']) : null,
        );

  @override
  int get hashCode => userId.hashCode ^ timestamp.hashCode ^ text.hashCode ^ keywords.hashCode;

  @override
  bool operator ==(other) {
    if (other is! Post) return false;
    Post post = other;
    return (post.userId == userId &&
        post.timestamp == timestamp &&
        post.text == text &&
        ListEquality().equals(post.keywords, keywords));
  }

  Map<String, dynamic> toJson() => {
        'userId': userId,
        'timestamp': timestamp?.toIso8601String(),
        'text': text,
        'keywords': keywords,
      };
}
