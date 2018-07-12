flo-runner
----------

A [flo] evaluation context with a backing service for persisting execution history and traces.

[flo]: https://github.com/spotify/flo


## Quickstart

Add the Maven, SBT or Gradle dependency

```
<dependency>
  <groupId>com.spotify</groupId>
  <artifactId>flo-runner</artifactId>
  <version>0.2.11</version>
</dependency>
```

```
libraryDependencies += "com.spotify" % "flo-runner" % "0.2.11"
```

```
compile 'com.spotify:flo-runner:0.2.11"'
```

If you're using the Scala API for [flo], also add

```
libraryDependencies += "com.spotify" %% "flo-scala" % "0.2.11"
```

Note: currently only supports scala version `2.11`.

## Usage - FloRunner

```scala
import com.spotify.flo._
import com.spotify.flo.context.FloRunner

object MyWorkflow {

  def exampleTask(greet: String): Task[String] = defTask() process {
    s"hello $greet"
  }

  def main(args: Array[String]): Unit = {
    val task = exampleTask("world")

    FloRunner.runTask(task).waitAndExit()
  }
}
```

### Running

Given that the above example has been assembled into an executable jar, we can run it using:

```sh
> java -jar target/example.jar
00:38:56.111 | INFO | FloRunner  |> Runner v0.1.2-SNAPSHOT
00:38:56.114 | INFO | FloRunner  |>
00:38:56.120 | INFO | FloRunner  |> Evaluation plan:
00:38:56.121 | INFO | FloRunner  |> exampleTask()#fd6443ec
00:38:56.121 | INFO | FloRunner  |>
00:38:56.126 | INFO | FloRunner  |> exampleTask()#fd6443ec Running ...
00:38:56.134 | INFO | FloRunner  |> exampleTask()#fd6443ec Completed -> hello world
00:38:56.142 | INFO | FloRunner  |> Total time 0:00:00.047
```

`FloRunner` supports some additional modes that allow some inspection of the task graph that have
been passed to `runTask`. The mode and other configuration is set through system properties when 
running the jar (`java -Dproperty=value <jar>`).

| property | behaviour |
|:---:|---|
| **`-Dflo.forking=true`** | Fork a subprocess for each task. |
| **`-Dflo.workers=n`** | Use `n` workers for running tasks concurrently. |
| **`-Dmode=tree`** | Only print the Evaluation plan and exit. |

## Utilities

There's some useful utilities in the `com.spotify.flo.util` package.

### Date and Time parameters

Tasks are typically parameterized by a `Date` or `DataHour`. The two corresponding types are 
useful for parsing and using these kind of parameters in your tasks.

```scala
val dateHour = DateHour.parse(args(0))
val task = exampleTask(dateHour)
```
