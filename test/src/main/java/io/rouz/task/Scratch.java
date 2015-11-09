package io.rouz.task;

import io.rouz.task.dsl.TaskBuilder;
import io.rouz.task.proc.Exec;

import java.io.IOException;

import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpecBuilder;

import static java.util.Arrays.asList;
import static java.util.Objects.requireNonNull;

/**
 * Task definitions have (TD)
 *  - a unique name
 *  - a list of parameters of specific types
 *  - an output type
 *
 * Task instances have (TI)
 *  - a TD
 *  - specific values for all TD parameters
 *  - a list of lazy input TI
 *  - code for producing the output
 *
 * TI are memoized on their TaskId which is determined only by the parameters.
 * Lazy input TI is essential
 *
 * Notes
 *  - the tricky part seems to lie in propagating depended task results to the dependent task
 *  --- main issue is in type safety, can easily be done with hashmaps
 *  - creating task instances should only yield a full dependency tree based on the task parameters
 *  - execution of tasks is subject to memoization
 *
 *  - can the setup, with dynamic dependencies be done with type-safe code?
 *  --- maybe with a value+trait based approach rather than classes+inheritance
 *
 *  FIXED: with {@link TaskBuilder} fluent api
 *
 *  - facts matching as basis for dependency satisfaction
 */
public class Scratch {

  public static void main(String[] args) throws IOException {
    OptionParser parser = parser();
    OptionSet parse = parser.parse(args);

    if (parse.has("h")) {
      parser.printHelpOn(System.err);
      System.exit(1);
    }

    System.out.println("parse.asMap() = " + parse.asMap());
    System.out.println("parse.has(\"parameter\") = " + parse.has("parameter"));
    System.out.println("parse.has(\"number\") = " + parse.has("number"));
    System.out.println("parse.has(\"wink\") = " + parse.has("wink"));

    System.out.println("parameter = " + requireNonNull(parse.valueOf("parameter")));
    System.out.println("parameter = " + parse.valueOf("parameter").getClass());
    System.out.println("number = " + (int) requireNonNull(parse.valueOf("number")));
    System.out.println("number = " + parse.valueOf("number").getClass());

//    Cli.forFactories(FloRootTaskFactory::exec).run(args);
  }

//  @RootTask
  static Task<Exec.Result> exec(String parameter, int number) {
    Task<String> task1 = MyTask.create(parameter);
    Task<Integer> task2 = Adder.create(number, number + 2);

    return Task.named("exec", "/bin/sh")
        .in(() -> task1)
        .in(() -> task2)
        .process(Exec.exec((str, i) -> args("/bin/sh", "-c", "\"echo " + i + "\"")));
  }

  static OptionParser parser() {
    final OptionParser parser = new OptionParser();

    opt("parameter", String.class, parser);
    opt("number", int.class, parser);
    opt("wink", boolean.class, parser);

    parser.acceptsAll(asList("h", "help")).forHelp();

    return parser;
  }

  static void opt(String name, Class<?> type, OptionParser parser) {
    final boolean isFlag = boolean.class.equals(type);
    final OptionSpecBuilder spec = (isFlag)
        ? parser.accepts(name, "(default: false)")
        : parser.accepts(name);

    if (!isFlag) {
      spec.withRequiredArg().ofType(type).describedAs(name).required();
    }
  }

  private static String[] args(String... args) {
    return args;
  }

  static class MyTask {
    static final int PLUS = 10;

    static Task<String> create(String parameter) {
      return Task.named("MyTask", parameter)
          .in(() -> Adder.create(parameter.length(), PLUS))
          .in(() -> Fib.create(parameter.length()))
          .process((sum, fib) -> something(parameter, sum, fib));
    }

    static String something(String parameter, int sum, int fib) {
      return "len('" + parameter + "') + " + PLUS + " = " + sum + ", " +
             "btw fib(" + parameter.length() + ") = "+ fib;
    }
  }

  static class Adder {
    static Task<Integer> create(int a, int b) {
      return Task.named("Adder", a, b).process(() -> a + b);
    }
  }

}
