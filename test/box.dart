import 'package:box/box.dart';
import 'package:box/mongodb.dart';
import 'package:collection/collection.dart';
import 'package:reflective/reflective.dart';
import 'package:test/test.dart';

main() {
  runTests('Memory', () => MemoryBox());
  runTests('File', () => FileBox('.box/test'));
  runTests('MongoDB', () => MongoDbBox('localhost', database: 'test'));
}

void runTests(String name, Box boxBuilder()) {
  Future<Box> reconnectIfPersistent(Box box) async {
    if (box.persistent) {
      await box.close();
      return boxBuilder();
    }
    return box;
  }

  var john = User(id: 'jdoe', name: 'John Doe');

  setUp(() async {
    var box = boxBuilder();
    await box.deleteAll<User>();
    await box.deleteAll<Post>();
  });

  group('$name - Find by key', () {
    test('Find by single key', () async {
      var box = boxBuilder();
      expect(await box.find<User>('jdoe'), isNull);

      User user = john;
      await box.store(user);

      box = await reconnectIfPersistent(box);
      expect(await box.find<User>('jdoe'), equals(user));
    });

    test('Find by composite key', () async {
      var box = boxBuilder();
      await box.deleteAll<Post>();
      User user = john;
      DateTime timestamp = DateTime.parse('2014-12-11T10:09:08Z');
      expect(await box.find<Post>({'userId': user.id, 'timestamp': timestamp}), isNull);

      Post post = Post(
          userId: user.id,
          timestamp: timestamp,
          text: 'I just discovered dart-box\nIt\'s awesome!',
          keywords: ['persistence', 'dart']);
      await box.store(post);

      box = await reconnectIfPersistent(box);
      Post found = await box.find<Post>({'userId': user.id, 'timestamp': timestamp});
      expect(found, equals(post));
    });
  });

  group('$name - Predicates', () {
    test('equals predicate, unique', () async {
      User jdoe = john;
      User crollis = User(id: 'crollis', name: 'Christine Rollis');
      User cstone = User(id: 'cstone', name: 'Cora Stone');
      User dsnow = User(id: 'dsnow', name: 'Donovan Snow');
      User koneil = User(id: 'koneil', name: 'Kendall Oneil');
      var box = boxBuilder();
      await box.storeAll([jdoe, crollis, cstone, dsnow, koneil]);

      box = await reconnectIfPersistent(box);
      expect((await box.selectFrom<User>().where('name').equals('Cora Stone').unique()), equals(cstone));
    });

    test('like predicate, list, order by', () async {
      User jdoe = john;
      User crollis = User(id: 'crollis', name: 'Christine Rollis');
      User cstone = User(id: 'cstone', name: 'Cora Stone');
      User dsnow = User(id: 'dsnow', name: 'Donovan Snow');
      User koneil = User(id: 'koneil', name: 'Kendall Oneil');
      var box = boxBuilder();
      await box.storeAll([jdoe, crollis, cstone, dsnow, koneil]);

      box = await reconnectIfPersistent(box);
      expect(await box.selectFrom<User>().where('name').like('C%').orderBy('name').ascending().list(),
          equals([crollis, cstone]));
    });

    test('gt predicate', () async {
      User jdoe = john;
      User crollis = User(id: 'crollis', name: 'Christine Rollis');
      User cstone = User(id: 'cstone', name: 'Cora Stone');
      User dsnow = User(id: 'dsnow', name: 'Donovan Snow');
      User koneil = User(id: 'koneil', name: 'Kendall Oneil');
      var box = boxBuilder();
      await box.storeAll([jdoe, crollis, cstone, dsnow, koneil]);

      box = await reconnectIfPersistent(box);
      expect((await box.selectFrom<User>().where('name').gt('Cora Stone').orderBy('name').ascending().list()),
          equals([dsnow, jdoe, koneil]));
    });

    test('gte predicate', () async {
      User jdoe = john;
      User crollis = User(id: 'crollis', name: 'Christine Rollis');
      User cstone = User(id: 'cstone', name: 'Cora Stone');
      User dsnow = User(id: 'dsnow', name: 'Donovan Snow');
      User koneil = User(id: 'koneil', name: 'Kendall Oneil');
      var box = boxBuilder();
      await box.storeAll([jdoe, crollis, cstone, dsnow, koneil]);

      box = await reconnectIfPersistent(box);
      expect((await box.selectFrom<User>().where('name').gte('Cora Stone').orderBy('name').ascending().list()),
          equals([cstone, dsnow, jdoe, koneil]));
    });

    test('lt predicate', () async {
      User jdoe = john;
      User crollis = User(id: 'crollis', name: 'Christine Rollis');
      User cstone = User(id: 'cstone', name: 'Cora Stone');
      User dsnow = User(id: 'dsnow', name: 'Donovan Snow');
      User koneil = User(id: 'koneil', name: 'Kendall Oneil');
      var box = boxBuilder();
      await box.storeAll([jdoe, crollis, cstone, dsnow, koneil]);

      box = await reconnectIfPersistent(box);
      expect((await box.selectFrom<User>().where('name').lt('Donovan Snow').orderBy('name').ascending().list()),
          equals([crollis, cstone]));
    });

    test('gte predicate', () async {
      User jdoe = john;
      User crollis = User(id: 'crollis', name: 'Christine Rollis');
      User cstone = User(id: 'cstone', name: 'Cora Stone');
      User dsnow = User(id: 'dsnow', name: 'Donovan Snow');
      User koneil = User(id: 'koneil', name: 'Kendall Oneil');
      var box = boxBuilder();
      await box.storeAll([jdoe, crollis, cstone, dsnow, koneil]);

      box = await reconnectIfPersistent(box);
      expect((await box.selectFrom<User>().where('name').lte('Donovan Snow').orderBy('name').ascending().list()),
          equals([crollis, cstone, dsnow]));
    });
  });

  group('$name - Operators', () {
    test('AND', () async {
      User jdoe = john;
      User crollis = User(id: 'crollis', name: 'Christine Rollis');
      User cstone = User(id: 'cstone', name: 'Cora Stone');
      User dsnow = User(id: 'dsnow', name: 'Donovan Snow');
      User koneil = User(id: 'koneil', name: 'Kendall Oneil');
      var box = boxBuilder();
      await box.storeAll([jdoe, crollis, cstone, dsnow, koneil]);

      box = await reconnectIfPersistent(box);
      expect(await box.selectFrom<User>().where('name').like('C%').and('name').like('%Stone').list(), equals([cstone]));
    });

    test('OR', () async {
      User jdoe = john;
      User crollis = User(id: 'crollis', name: 'Christine Rollis');
      User cstone = User(id: 'cstone', name: 'Cora Stone');
      User dsnow = User(id: 'dsnow', name: 'Donovan Snow');
      User koneil = User(id: 'koneil', name: 'Kendall Oneil');
      var box = boxBuilder();
      await box.storeAll([jdoe, crollis, cstone, dsnow, koneil]);

      box = await reconnectIfPersistent(box);
      expect(
          await box
              .selectFrom<User>()
              .where('name')
              .like('Cora%')
              .or('name')
              .like('%Snow')
              .orderBy('name')
              .ascending()
              .list(),
          equals([cstone, dsnow]));
    });

    test('NOT, descending', () async {
      User jdoe = john;
      User crollis = User(id: 'crollis', name: 'Christine Rollis');
      User cstone = User(id: 'cstone', name: 'Cora Stone');
      User dsnow = User(id: 'dsnow', name: 'Donovan Snow');
      User koneil = User(id: 'koneil', name: 'Kendall Oneil');
      var box = boxBuilder();
      await box.storeAll([jdoe, crollis, cstone, dsnow, koneil]);

      box = await reconnectIfPersistent(box);
      expect(
          await box.selectFrom<User>().where('name').not().equals('Donovan Snow').orderBy('name').descending().list(),
          equals([koneil, jdoe, cstone, crollis]));
    });
  });

  group('$name - Deep queries', () {
    test('Deep query into value object', () async {
      User crollis =
          User(id: 'crollis', name: 'Christine Rollis', lastPost: Post(text: 'Dart 2.6.1 is out!', keywords: ['dart']));
      User cstone = User(
          id: 'cstone',
          name: 'Cora Stone',
          lastPost: Post(text: 'Cupcakes are ready!', keywords: ['baking', 'cupcakes']));
      User dsnow = User(
          id: 'dsnow',
          name: 'Donovan Snow',
          lastPost: Post(text: 'I just discovered dart-box\nIt\'s awesome!', keywords: ['persistence', 'dart']));
      User koneil = User(
          id: 'koneil',
          name: 'Kendall Oneil',
          lastPost: Post(text: 'Has anyone seen my dog?', keywords: ['dog', 'lost']));
      var box = boxBuilder();
      await box.storeAll([crollis, cstone, dsnow, koneil]);

      box = await reconnectIfPersistent(box);
      expect(await box.selectFrom<User>().where('lastPost.text').like('%dart%').orderBy('name').ascending().list(),
          equals([crollis, dsnow]));
    });
  });
}

class User {
  @key
  String id;
  String name;
  Post lastPost;
  List<Post> posts;

  User({this.id, this.name, this.lastPost, this.posts});

  String toString() => '@' + id + ' (' + name + ')';

  int get hashCode => Objects.hash([id, name]);

  bool operator ==(other) {
    if (other is! User) return false;
    User user = other;
    return (user.id == id && user.name == name);
  }
}

class Post {
  @key
  String userId;
  @key
  DateTime timestamp;
  String text;
  List<String> keywords;

  Post({this.userId, this.timestamp, this.text, this.keywords});

  int get hashCode => Objects.hash([userId, timestamp, text]);

  bool operator ==(other) {
    if (other is! Post) return false;
    Post post = other;
    return (post.userId == userId &&
        post.timestamp == timestamp &&
        post.text == text &&
        ListEquality().equals(post.keywords, keywords));
  }
}
