import 'package:box/box.dart';
import 'package:box/mongodb.dart';
import 'package:reflective/reflective.dart';
import 'package:test/test.dart';

main() {
  runTests('Memory', MemoryBox());
  runTests('File', FileBox('.box/test'));
  runTests('MongoDB', MongoDbBox('localhost', database: 'test'));
}

void runTests(String name, Box box) {
  var john = User(id: 'jdoe', name: 'John Doe');
  group(name, () {
    setUp(() async {
      await box.deleteAll<User>();
    });

    test('Find by single key', () async {
      expect(await box.find<User>('jdoe'), isNull);

      User user = john;
      await box.store(user);

      expect(await box.find<User>('jdoe'), equals(user));
    });

    test('Find by composite key', () async {
      await box.deleteAll<Post>();
      User user = john;
      DateTime timestamp = DateTime.parse('2014-12-11T10:09:08Z');
      expect(await box.find<Post>({'userId': user.id, 'timestamp': timestamp}), isNull);

      Post post = Post(userId: user.id, timestamp: timestamp, text: 'I just discovered dart-box\nIt\'s awesome!');
      await box.store(post);
      Post found = await box.find<Post>({'userId': user.id, 'timestamp': timestamp});
      //expect(found, equals(post));
    });

    test('Equals predicate, unique', () async {
      User jdoe = john;
      User crollis = User(id: 'crollis', name: 'Christine Rollis');
      User cstone = User(id: 'cstone', name: 'Cora Stone');
      User dsnow = User(id: 'dsnow', name: 'Donovan Snow');
      User koneil = User(id: 'koneil', name: 'Kendall Oneil');
      await box.store(jdoe);
      await box.store(crollis);
      await box.store(cstone);
      await box.store(dsnow);
      await box.store(koneil);

      expect((await box.selectFrom<User>().where('name').equals('Cora Stone').unique()), equals(cstone));
    });

    test('Like predicate, list, order by', () async {
      User jdoe = john;
      User crollis = User(id: 'crollis', name: 'Christine Rollis');
      User cstone = User(id: 'cstone', name: 'Cora Stone');
      User dsnow = User(id: 'dsnow', name: 'Donovan Snow');
      User koneil = User(id: 'koneil', name: 'Kendall Oneil');
      await box.store(jdoe);
      await box.store(crollis);
      await box.store(cstone);
      await box.store(dsnow);
      await box.store(koneil);

      expect(await box.selectFrom<User>().where('name').like('C%').orderBy('name').ascending().list(),
          equals([crollis, cstone]));
    });

    test('AND', () async {
      User jdoe = john;
      User crollis = User(id: 'crollis', name: 'Christine Rollis');
      User cstone = User(id: 'cstone', name: 'Cora Stone');
      User dsnow = User(id: 'dsnow', name: 'Donovan Snow');
      User koneil = User(id: 'koneil', name: 'Kendall Oneil');
      await box.store(jdoe);
      await box.store(crollis);
      await box.store(cstone);
      await box.store(dsnow);
      await box.store(koneil);

      expect(await box.selectFrom<User>().where('name').like('C%').and('name').like('%Stone').list(), equals([cstone]));
    });

    test('OR', () async {
      User jdoe = john;
      User crollis = User(id: 'crollis', name: 'Christine Rollis');
      User cstone = User(id: 'cstone', name: 'Cora Stone');
      User dsnow = User(id: 'dsnow', name: 'Donovan Snow');
      User koneil = User(id: 'koneil', name: 'Kendall Oneil');
      await box.store(jdoe);
      await box.store(crollis);
      await box.store(cstone);
      await box.store(dsnow);
      await box.store(koneil);

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
      await box.store(jdoe);
      await box.store(crollis);
      await box.store(cstone);
      await box.store(dsnow);
      await box.store(koneil);

      expect(
          await box.selectFrom<User>().where('name').not().equals('Donovan Snow').orderBy('name').descending().list(),
          equals([koneil, jdoe, cstone, crollis]));
    });
  });
}

class User {
  @key
  String id;
  String name;

  User({this.id, this.name});

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

  Post({this.userId, this.timestamp, this.text});

  int get hashCode => Objects.hash([userId, timestamp, text]);

  bool operator ==(other) {
    if (other is! Post) return false;
    Post post = other;
    return (post.userId == userId && post.timestamp == timestamp && post.text == text);
  }
}
