flo-core
========

### Definitions

- `t<x>` - a `Task` of type `x`
- `fN` - a function of N arguments with type `(...n) -> z`
- `[ … ]` - an evaluation within the `value<x>` "box" of a `task<x>`
- `builderN` - the builder at N inputs, with internal reference

### Builders

Let's start by looking at a builder with one input.

```
builder1 = flo.task("foo").in(task<a>)
```

The above expression (or equivalent in the Java or Scala API) evaluates to a builder that
internally has the following structure.

```
builder1 ─┐
          └ ┌                  ┐
task<a> ───>│ a -> f1 -> f1(a) │
            └                  ┘
```

A builder for a task of one input, has a reference to the evaluation "box" for `task<a>`'s value
`a`.

The function `f1` (`a -> z`) a placeholder for the function given to `builder1.process(f1)` which
finalizes the builder and produces a `task<z>`.

When `task<z>` is evaluated, `f1` is called with `a` from the input task as soon as it has
finished evaluating. This will produce a `value<z>` which contains the value returned by `f1`, or
any exception thrown along the dependecy tree.

If instead `builder1.in(task<b>)` is called, a `builder2` is returned, which chains onto the
evaluation "box" from the first builder.

```
builder1 ─┐
          └ ┌                  ┐
task<a> ───>│ a -> f1 -> f1(a) │
          ┌ └      ^           ┘
builder2 ─┤        └──────┐
          └ ┌             ╵             ┐
task<b> ───>│ b -> f2 -> (a) -> f2(a,b) │
            └                           ┘
```

_describe chaining_

```
builder1 ─┐
          └ ┌                  ┐
task<a> ───>│ a -> f1 -> f1(a) │
          ┌ └      ^           ┘
builder2 ─┤        └──────┐
          └ ┌             ╵             ┐
task<b> ───>│ b -> f2 -> (a) -> f2(a,b) │
          ┌ └      ^                    ┘
builder3 ─┤        └──────┐
          └ ┌             ╵                 ┐
task<c> ───>│ c -> f3 -> (a,b) -> f3(a,b,c) │
          ┌ └      ^                        ┘
          ┆        ┆
builderN ─┤        └──────┐
          └ ┌             ╵                             ┐
task<n> ───>│ n -> fn -> (a,b,…,n-1) -> fn(a,b,…,n-1,n) │
            └                                           ┘
```
