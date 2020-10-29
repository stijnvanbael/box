Dart Box
========

A fluent Dart persistence API inspired by SQL.
This library is still in an early phase of development.
Currently Box has following implementations:

  * MemoryBox: a very simple implementation that runs completely in-memory, ideal for unit tests and caching.
  * FileBox: an in-memory implementation that persists data to a simple JSON file, useful for simple applications with a low concurrency.
  * MongoDbBox
  * FirestoreBox: this implementation has some limitations, it does not support composite keys, LIKE, NOT, OR and IN.
  * PostgresBox: restricted to JSON types for lists and nested entities, it does not support arrays and complex types.

Box requires a registry to be generated from the entity model.
See [box_generator](https://github.com/stijnvanbael/box_generator/blob/master/README.md) how to do so.


Example:

    var registry = Registry()..register(User$BoxSupport());
    var box = FileBox('.box/test', registry);
    
    var users = await box.selectFrom<User>()
                                .where('name').like('C%')
                                .orderBy('name').ascending()
                                .list();
                                
    users.forEach((user) => print(user.name));

