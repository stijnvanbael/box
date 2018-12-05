import 'dart:io';

import 'package:box/box.dart';
import 'package:reflective/reflective.dart';
import 'package:test/test.dart';

main() {
  var john = User(handle: 'jdoe', name: 'John Doe');
  var margaret = User(handle: 'mdoe', name: 'Margaret Doe');
  var emma = User(handle: 'edoe', name: 'Emma Doe');

  group('File-based', () {
    Box box;

    test('Store and retrieve simple entity by a single key', () async {
      File file = File('.box/test/User');
      if (file.existsSync()) {
        file.deleteSync();
      }
      box = FileBox('.box/test');
      expect(await box.find<User>('jdoe'), isNull);

      await box.store(john);
      box = FileBox('.box/test');
      User found = await box.find<User>('jdoe');
      expect(found, equals(john));
    });

    test('Store and retrieve simple entity by a composite key', () async {
      File file = File('.box/test/Post');
      if (file.existsSync()) {
        file.deleteSync();
      }
      box = FileBox('.box/test');
      User user = john;
      DateTime timestamp = DateTime.parse('2014-12-11T10:09:08Z');
      expect(await box.find<Post>([user, timestamp]), isNull);

      Post post = Post(
          user: user,
          timestamp: timestamp,
          text: 'I just discovered dart-box\nIt\'s awesome!');
      await box.store(post);
      box = FileBox('.box/test');
      Post found = await box.find<Post>([user, timestamp]);
      expect(found, equals(post));
    });

    test('Store multiple entities and query', () async {
      File file = File('.box/test/box.test.User');
      if (file.existsSync()) {
        file.deleteSync();
      }
      box = FileBox('.box/test');

      await box.store(john);
      await box.store(margaret);
      await box.store(emma);

      box = FileBox('.box/test');
      List<User> users =
          await box.selectFrom<User>().where('name').like('%Doe').list();
      expect(users, [john, margaret, emma]);
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
