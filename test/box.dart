library box.test;

import 'dart:io';
import 'package:unittest/unittest.dart';
import 'package:box/box.dart';
import 'package:reflective/reflective.dart';

main() {
  group('In-memory', () {
    Box box;

    setUp(() {
      box = new Box();
    });

    test('Store and retrieve simple entity by a single key', () async {
      expect(await box.find(User, 'jdoe'), isNull);

      User user = new User(handle: 'jdoe', name: 'John Doe');
      box.store(user);

      expect(await box.find(User, 'jdoe'), equals(user));
    });

    test('Store and retrieve simple entity by a composite key', () async {
      User user = new User(handle: 'jdoe', name: 'John Doe');
      DateTime timestamp = DateTime.parse('2014-12-11T10:09:08Z');
      expect(await box.find(Post, [user, timestamp]), isNull);

      Post post = new Post(
          user: user,
          timestamp: timestamp,
          text: 'I just discovered dart-box, it\'s awesome!');
      box.store(post);

      expect(await box.find(Post, [user, timestamp]), equals(post));
    });

    test('Equals predicate, unique', () async {
      User jdoe = new User(handle: 'jdoe', name: 'John Doe');
      User crollis = new User(handle: 'crollis', name: 'Christine Rollis');
      User cstone = new User(handle: 'cstone', name: 'Cora Stone');
      User dsnow = new User(handle: 'dsnow', name: 'Donovan Snow');
      User koneil = new User(handle: 'koneil', name: 'Kendall Oneil');
      box.store(jdoe);
      box.store(crollis);
      box.store(cstone);
      box.store(dsnow);
      box.store(koneil);

      expect((await box.query(User)
      .where('name').equals('Cora Stone')
      .unique()).get(),
      equals(cstone));
    });

    test('Like predicate, list, order by', () async {
      User jdoe = new User(handle: 'jdoe', name: 'John Doe');
      User crollis = new User(handle: 'crollis', name: 'Christine Rollis');
      User cstone = new User(handle: 'cstone', name: 'Cora Stone');
      User dsnow = new User(handle: 'dsnow', name: 'Donovan Snow');
      User koneil = new User(handle: 'koneil', name: 'Kendall Oneil');
      box.store(jdoe);
      box.store(crollis);
      box.store(cstone);
      box.store(dsnow);
      box.store(koneil);

      expect(await box.query(User)
      .where('name').like('C%')
      .orderBy('name').ascending()
      .list(),
      equals([crollis, cstone]));
    });

    test('not, descending', () async {
      User jdoe = new User(handle: 'jdoe', name: 'John Doe');
      User crollis = new User(handle: 'crollis', name: 'Christine Rollis');
      User cstone = new User(handle: 'cstone', name: 'Cora Stone');
      User dsnow = new User(handle: 'dsnow', name: 'Donovan Snow');
      User koneil = new User(handle: 'koneil', name: 'Kendall Oneil');
      box.store(jdoe);
      box.store(crollis);
      box.store(cstone);
      box.store(dsnow);
      box.store(koneil);

      expect(await box.query(User)
      .where('name').not().equals('Donovan Snow')
      .orderBy('name').descending()
      .list(),
      equals([koneil, jdoe, cstone, crollis]));
    });
  });

  group('File-based', () {
    Box box;

    test('Store and retrieve simple entity by a single key', () async {
      File file = new File('.box/test/box.test.User');
      if (file.existsSync()) {
        file.deleteSync();
      }
      box = new Box.file('.box/test');
      expect(await box.find(User, 'jdoe'), isNull);

      User user = new User(handle: 'jdoe', name: 'John Doe');
      User found = await box.store(user).then((result) async {
        box = new Box.file('.box/test');
        return await box.find(User, 'jdoe');
      });
      expect(found, equals(user));
    });

    test('Store and retrieve simple entity by a composite key', () async {
      File file = new File('.box/test/box.test.Post');
      if (file.existsSync()) {
        file.deleteSync();
      }
      box = new Box.file('.box/test');
      User user = new User(handle: 'jdoe', name: 'John Doe');
      DateTime timestamp = DateTime.parse('2014-12-11T10:09:08Z');
      expect(await box.find(Post, [user, timestamp]), isNull);

      Post post = new Post(
          user: user,
          timestamp: timestamp,
          text: 'I just discovered dart-box, it\'s awesome!');
      Post found = await box.store(post).then((result) async {
        box = new Box.file('.box/test');
        return await box.find(Post, [user, timestamp]);
      });
      expect(found, equals(post));
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
    return (post.user == user && post.timestamp == timestamp && post.text == text);
  }
}