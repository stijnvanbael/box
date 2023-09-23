PatternMatcher<I, O> matcher<I, O>() => PatternMatcher([]);

TransformingPredicate<I, T> predicate<I, T>(bool Function(I input) predicate,
    [T Function(I input)? transformer, String? description]) {
  transformer = transformer ?? identity;
  return TransformingPredicate<I, T>(predicate, transformer, description);
}

T identity<T>(dynamic input) => input as T;

class PatternMatcher<I, O> {
  final List<_Case> _cases;

  PatternMatcher(this._cases);

  PatternMatcher<I, O> whenIs<T>(O Function(T input) function) =>
      when(typeIs<T>() as TransformingPredicate<I, T>, function);

  PatternMatcher<I, O> whenEquals<T>(T value, O Function(T input) function) =>
      when(equals(value), function);

  PatternMatcher<I, O> whenNull(O Function(I input) function) =>
      when(isNull(), function);

  PatternMatcher<I, O> when<T>(
      TransformingPredicate<I, T> predicate, O Function(T input) function) {
    var newCases = List<_Case<dynamic, dynamic, dynamic>>.from(_cases);
    newCases.add(_Case(predicate, function));
    return PatternMatcher(newCases);
  }

  PatternMatcher<I, O> when2<T1, T2>(
      TransformingPredicate<I, Pair<T1, T2>> predicate,
      O Function(T1 input1, T2 input2) function) {
    var newCases = List<_Case<dynamic, dynamic, dynamic>>.from(_cases);
    newCases.add(_Case(predicate, (p) => function(p.a, p.b)));
    return PatternMatcher(newCases);
  }

  PatternMatcher<I, O> otherwise(O Function(I input) function) =>
      when<I>(predicate((i) => true, identity, 'Otherwise'), function);

  O? apply(I input) => call(input);

  O? call(I input) {
    for (var c in _cases) {
      if (c.matches(input)) {
        return c(input);
      }
    }
    return null;
  }
}

class _Case<I, T, O> {
  final TransformingPredicate<I, T> _transformingPredicate;
  final Function _function;

  _Case(this._transformingPredicate, this._function);

  bool matches(I input) => _transformingPredicate.test(input);

  O call(I input) {
    var transformed = _transformingPredicate.transform(input);
    var applied = _function(transformed);
    return applied;
  }

  @override
  String toString() => _transformingPredicate.toString();
}

class Pair<A, B> {
  final A a;
  final B b;

  Pair(this.a, this.b);
}

class TransformingPredicate<I, T> {
  final Function _predicate;
  final Function _transformer;
  final String? _description;

  TransformingPredicate(
      bool Function(I input) predicate, T Function(I input) transformer,
      [String? description])
      : _predicate = predicate,
        _transformer = transformer,
        _description = description;

  bool test(I input) => _predicate(input);

  T transform(I input) {
    return _transformer(input);
  }

  @override
  String toString() {
    return _description ?? 'TransformingPredicate';
  }
}

TransformingPredicate<Object, T> typeIs<T>() =>
    TransformingPredicate<Object, T>((i) => i is T, identity);

TransformingPredicate<I, T> any<I, T>(
        List<TransformingPredicate<I, T>> predicates) =>
    TransformingPredicate<I, T>(
        (i) => predicates.any((p) => p.test(i)), identity);

TransformingPredicate<I, T> equals<I, T>(T? value) =>
    TransformingPredicate<I, T>((i) => i == value, identity);

TransformingPredicate<I, I> isNull<I>() => equals(null);
