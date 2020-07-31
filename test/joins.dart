import 'package:box/box.dart';
import 'package:box/firestore.dart';
import 'package:box/mongodb.dart';
import 'package:box/postgres.dart';
import 'package:test/test.dart';

import 'box.dart';

void joinTests(String name, Box Function() boxBuilder) async {
  Future<Box> reconnectIfPersistent(Box box) async {
    if (box != firestore && box.persistent) {
      await box.close();
      return boxBuilder();
    }
    return box;
  }

  Future<Box> setUp() async {
    var box = boxBuilder();
    await box.deleteAll<User>();
    await box.deleteAll<Post>();
    return box;
  }

  group('$name - Joins', () {
    test('Simple inner join', () async {
      var crollis = User(id: 'crollis', name: 'Christine Rollis');
      var cstone = User(id: 'cstone', name: 'Cora Stone', lastPost: Post(text: 'Signing off'));
      var dsnow = User(id: 'dsnow', name: 'Donovan Snow', lastPost: Post(text: 'Hi!'));
      var box = await setUp();
      await box.storeAll([crollis, cstone, dsnow]);
      var crollisPost = Post(userId: 'crollis', timestamp: DateTime.parse('2020-05-01T12:13:14Z'), text: 'Bye!');
      await box.store(crollisPost);
      await box.store(Post(userId: 'cstone', timestamp: DateTime.parse('2020-05-01T13:14:15Z'), text: 'Signing off'));
      await box.store(Post(userId: 'dsnow', timestamp: DateTime.parse('2020-05-02T08:09:10Z'), text: 'Hi!'));

      box = await reconnectIfPersistent(box);
      expect(
          await box
              .selectFrom(User)
              .innerJoin(Post)
              .on('User.id')
              .equals('Post.userId')
              .where('User.id')
              .equals('crollis')
              .list(),
          equals([
            {
              'User': crollis,
              'Post': crollisPost,
            }
          ]));
    });

    test('Inner join with aliases', () async {
      var crollis = User(id: 'crollis', name: 'Christine Rollis');
      var cstone = User(id: 'cstone', name: 'Cora Stone', lastPost: Post(text: 'Signing off'));
      var dsnow = User(id: 'dsnow', name: 'Donovan Snow', lastPost: Post(text: 'Hi!'));
      var box = await setUp();
      await box.storeAll([crollis, cstone, dsnow]);
      var crollisPost = Post(userId: 'crollis', timestamp: DateTime.parse('2020-05-01T12:13:14Z'), text: 'Bye!');
      await box.store(crollisPost);
      await box.store(Post(userId: 'cstone', timestamp: DateTime.parse('2020-05-01T13:14:15Z'), text: 'Signing off'));
      await box.store(Post(userId: 'dsnow', timestamp: DateTime.parse('2020-05-02T08:09:10Z'), text: 'Hi!'));

      box = await reconnectIfPersistent(box);
      expect(
          await box
              .selectFrom(User, 'u')
              .innerJoin(Post, 'p')
              .on('u.id')
              .equals('p.userId')
              .where('u.id')
              .equals('crollis')
              .list(),
          equals([
            {
              'u': crollis,
              'p': crollisPost,
            }
          ]));
    });

    test('Multiple joins', () async {
      fail('TODO');
    });

    test('Join with self', () async {
      fail('TODO');
    });

    test('Left outer join', () async {
      fail('TODO');
    });

    test('Right outer join', () async {
      fail('TODO');
    });

    test('Full outer join', () async {
      fail('TODO');
    });

    test('Cross join', () async {
      fail('TODO');
    });

    test('Join with select fields', () async {
      fail('TODO');
    });

    test('Unknown entity in SELECT step', () async {
      var box = await setUp();
      expect(
          () => box
              .select([$('foo.name')])
              .from(User, 'u')
              .innerJoin(Post, 'p')
              .on('u.id')
              .equals('p.userId')
              .where('u.id')
              .equals('crollis'),
          throwsA((e) => e is ArgumentError && e.message == 'Unknown entity or alias "foo" in SELECT step'));
    });

    test('Unknown entity in ON step', () async {
      var box = await setUp();
      expect(
          () => box
              .selectFrom(User, 'u')
              .innerJoin(Post, 'p')
              .on('foo.id')
              .equals('p.userId')
              .where('u.id')
              .equals('crollis'),
          throwsA((e) => e is ArgumentError && e.message == 'Unknown entity or alias "foo" in ON step'));
    });

    test('Unknown field in ON step', () async {
      var box = await setUp();
      expect(
              () => box
              .selectFrom(User, 'u')
              .innerJoin(Post, 'p')
              .on('u.foo')
              .equals('p.userId')
              .where('u.id')
              .equals('crollis'),
          throwsA((e) => e is ArgumentError && e.message == 'Unknown field "User.foo" in ON step'));
    });

    test('Unknown entity in WHERE step', () async {
      var box = await setUp();
      expect(
          () => box
              .selectFrom(User, 'u')
              .innerJoin(Post, 'p')
              .on('u.id')
              .equals('p.userId')
              .where('foo.id')
              .equals('crollis'),
          throwsA((e) => e is ArgumentError && e.message == 'Unknown entity or alias "foo" in WHERE step'));
    });

    test('Unknown entity in ORDER BY step', () async {
      var box = await setUp();
      expect(
          () => box
              .selectFrom(User, 'u')
              .innerJoin(Post, 'p')
              .on('u.id')
              .equals('p.userId')
              .where('u.id')
              .equals('crollis')
              .orderBy('foo.name'),
          throwsA((e) => e is ArgumentError && e.message == 'Unknown entity or alias "foo" in ORDER BY step'));
    });
  });
}
