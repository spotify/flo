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
            └      ^           ┘
                   └─── builder1.process(f1)
```

A builder for a task of one input, has a reference to the evaluation "box" for `task<a>`'s value
`a`.

The function `f1` (`a -> z`) a placeholder for the function given to `builder1.process(f1)` which
finalizes the builder and produces a `task<z>`.

When `task<z>` is evaluated, `f1` is called with `a` from the input task as soon as it has
finished evaluating. This will produce a `value<z>` which contains the value returned by `f1`, or
any exception thrown along the dependency tree.

If instead `builder1.in(task<b>)` is called, a `builder2` is returned, which chains onto the
evaluation box from the first builder.

```
builder1 ─┐
          └ ┌                  ┐
task<a> ───>│ a -> f1 -> f1(a) │
          ┌ └      ^           ┘
builder2 ─┤        └──────┐
          └ ┌             ╵             ┐
task<b> ───>│ b -> f2 -> (a) -> f2(a,b) │
            └      ^                    ┘
                   └─── builder2.process(f2)
```

The next builder will chain the two evaluation boxes together, and within them pass a reduced
function to the previous box. The reduced function only takes one argument `a` and is within the
evaluation box of the second task, so it can invoke the `f2` passed to `builder2.process(f2)` as 
soon as both values `a` and `b` become available.

The chaining is done using [Values.mapBoth], see the javadoc for more details.

This process is repeated for each extension of the builder. In general, the builder will build up
something like the following structure.

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
            └      ^                                    ┘
                   └─── builderN.process(fn)
```

[Values.mapBoth]: https://github.com/spotify/flo/blob/master/workflow/src/main/java/com/spotify/flo/Values.java#L37
