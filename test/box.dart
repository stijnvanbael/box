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
      return box.store(user).then((result) async {
        box = new Box.file('.box/test');
        expect(await box.find(User, 'jdoe'), equals(user));
        return null;
      });

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
    User person = other;
    return (person.handle == handle &&
    person.name == name);
  }
}

class Post {
  @key
  User user;
  @key
  DateTime timestamp;
  String text;

  Post({this.user, this.timestamp, this.text});
}