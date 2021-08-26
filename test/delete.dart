import 'package:box/box.dart';
import 'package:box/firestore.dart';
import 'package:box/mongodb.dart';
import 'package:box/postgres.dart';
import 'package:test/test.dart';

import 'box.dart';

Future deleteTests(String name, Box Function() boxBuilder) async {
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

  group('$name - Delete', () {
    test('Delete with condition', () async {
      var crollis = User(id: 'crollis', name: 'Christine Rollis');
      var cstone = User(
          id: 'cstone',
          name: 'Cora Stone',
          lastPost: Post(text: 'Signing off'));
      var dsnow =
          User(id: 'dsnow', name: 'Donovan Snow', lastPost: Post(text: 'Hi!'));
      var box = await setUp();
      await box.storeAll([crollis, cstone, dsnow]);

      box = await reconnectIfPersistent(box);

      await box.deleteFrom(User).where('id').equals('crollis').execute();
      expect(await box.selectFrom<User>().list(), equals([cstone, dsnow]));
    });
  });
}
