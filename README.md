Dart Box
========

A fluent Dart persistence API inspired by SQL.
This library is still in an early phase of development.
Currently Box has following implementations.

  * MemoryBox: a very simple implementation that runs completely in-memory.
  * FileBox: an in-memory implementation that persists data to a simple JSON file.
  * MongoDbBox
  * FirestoreBox: this implementation has some limitations. Composite keys, like, not, or and oneOf are not supported yet.
  * PostgresBox: restricted to JSON types for lists and nested entities. Arrays and complex types are not supported yet.

Support for various SQL and NoSQL databases is on the roadmap.

Box requires a registry to be generated from the entity model.
See https://github.com/stijnvanbael/box_generator/blob/master/README.md how to generate the registry.


Example:

    var registry = initBoxRegistry();
    var box = new FileBox('.box/test', registry);
    var users = await box.selectFrom<User>()
                                .where('name').like('C%')
                                .orderBy('name').ascending()
                                .list();
    users.forEach((user) => print(user.name)));

