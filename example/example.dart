import 'package:box/box.dart';

part 'example.g.dart';

void main() async {
  var registry = Registry()..register(User$BoxSupport());
  var box = FileBox('.box/test', registry);

  var users = await box.selectFrom<User>()
      .where('name').like('C%')
      .orderBy('name').ascending()
      .list();

  users.forEach((user) => print(user.name));
}

@entity
class User {
  @key
  final String id;
  final String name;
  final String email;

  User({this.id, this.name, this.email});
}
