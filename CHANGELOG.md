0.3.1+1 Return generated ID
---------------------------
* [ ] Return generated ID

0.3.1 Delete
------------
* [x] DELETE FROM

0.3.0+1 Latest versions
-----------------------
* [x] Bumped versions of dependencies

0.3.0 Easier mapping
--------------------
* [x] Automatic toJson and fromJson

0.2.6 PostgreSQL
----------------
* [x] PostgreSQL support

0.2.5 ONE OF & CONTAINS
-----------------------
* [x] ONE OF (IN) predicate
* [x] CONTAINS predicate for arrays

0.2.4 Firestore
---------------
* [x] Removed dependency on mirrors at runtime
* [x] Firestore support

0.2.3 Limit & Offset
--------------------
* [x] Limit
* [x] Offset
* [x] Select and map result

0.2.2 MongoDB
-------------
* [x] MongoDB support
* [x] Deep queries
* [x] Greater than (or equal)
* [x] Less than (or equal)
* [x] Between
* [x] Dynamically typed queries

Backlog
-------
* [ ] Convert types like enums and DateTime in queries and deletes
* [ ] Create indexes for memory, file, MongoDB and Firestore
* [ ] Group by
* [ ] Having
* [ ] Union
* [ ] Intersect
* [ ] Minus/Except
* [ ] Better test concern separation
* [ ] Misuse reporting
* [ ] Faster Firestore tests
* [ ] SQLite support
* [ ] MySQL support
* [ ] Emulate unsupported Firestore features
* [ ] Typesafe fields (eg: `select(employee.name).from(Employee).where(employee.department).equals('Sales')`)
* [ ] Joins
* [ ] Support PostgreSQL arrays and complex types (depends on driver issue: [postgresql-dart#121](https://github.com/stablekernel/postgresql-dart/issues/121))
