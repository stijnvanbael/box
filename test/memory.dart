import 'package:box/memory.dart';
import 'package:reflective/reflective.dart';
import 'package:test/test.dart';

main() {
  var john = User(handle: 'jdoe', name: 'John Doe');

  group('In-memory', () {
    Box box;

    setUp(() {
      box = MemoryBox();
    });

    test('Store and retrieve simple entity by a single key', () async {
      expect(await box.find<User>('jdoe'), isNull);

      User user = john;
      box.store(user);

      expect(await box.find<User>('jdoe'), equals(user));
    });

    test('Store and retrieve simple entity by a composite key', () async {
      User user = john;
      DateTime timestamp = DateTime.parse('2014-12-11T10:09:08Z');
      expect(await box.find<Post>([user, timestamp]), isNull);

      Post post = Post(
          user: user,
          timestamp: timestamp,
          text: 'I just discovered dart-box, it\'s awesome!');
      box.store(post);

      expect(await box.find<Post>([user, timestamp]), equals(post));
    });

    test('Equals predicate, unique', () async {
      User jdoe = john;
      User crollis = User(handle: 'crollis', name: 'Christine Rollis');
      User cstone = User(handle: 'cstone', name: 'Cora Stone');
      User dsnow = User(handle: 'dsnow', name: 'Donovan Snow');
      User koneil = User(handle: 'koneil', name: 'Kendall Oneil');
      box.store(jdoe);
      box.store(crollis);
      box.store(cstone);
      box.store(dsnow);
      box.store(koneil);

      expect(
          (await box
                  .selectFrom<User>()
                  .where('name')
                  .equals('Cora Stone')
                  .unique())
              .get(),
          equals(cstone));
    });

    test('Like predicate, list, order by', () async {
      User jdoe = john;
      User crollis = User(handle: 'crollis', name: 'Christine Rollis');
      User cstone = User(handle: 'cstone', name: 'Cora Stone');
      User dsnow = User(handle: 'dsnow', name: 'Donovan Snow');
      User koneil = User(handle: 'koneil', name: 'Kendall Oneil');
      box.store(jdoe);
      box.store(crollis);
      box.store(cstone);
      box.store(dsnow);
      box.store(koneil);

      expect(
          await box
              .selectFrom<User>()
              .where('name')
              .like('C%')
              .orderBy('name')
              .ascending()
              .list(),
          equals([crollis, cstone]));
    });

    test('not, descending', () async {
      User jdoe = john;
      User crollis = User(handle: 'crollis', name: 'Christine Rollis');
      User cstone = User(handle: 'cstone', name: 'Cora Stone');
      User dsnow = User(handle: 'dsnow', name: 'Donovan Snow');
      User koneil = User(handle: 'koneil', name: 'Kendall Oneil');
      box.store(jdoe);
      box.store(crollis);
      box.store(cstone);
      box.store(dsnow);
      box.store(koneil);

      expect(
          await box
              .selectFrom<User>()
              .where('name')
              .not()
              .equals('Donovan Snow')
              .orderBy('name')
              .descending()
              .list(),
          equals([koneil, jdoe, cstone, crollis]));
    });
  });
}

class User {
  @key
  String handle;
  String name;

  User({this.handle, this.name});

  String toString() => '@' + handle + ' (' + name + ')';

  int get hashCode => Objects.hash([handle, name]);

  bool operator ==(other) {
    if (other is! User) return false;
    User user = other;
    return (user.handle == handle && user.name == name);
  }
}

class Post {
  @key
  User user;
  @key
  DateTime timestamp;
  String text;

  Post({this.user, this.timestamp, this.text});

  int get hashCode => Objects.hash([user, timestamp, text]);

  bool operator ==(other) {
    if (other is! Post) return false;
    Post post = other;
    return (post.user == user &&
        post.timestamp == timestamp &&
        post.text == text);
  }
}
