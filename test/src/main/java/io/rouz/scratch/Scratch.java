package io.rouz.scratch;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import java.io.IOException;

import io.rouz.TaskInfo;
import io.rouz.task.Task;
import io.rouz.task.proc.Exec;
import io.rouz.task.processor.RootTask;

/**
 * Task definitions have (TD)
 *  - todo: namespace
 *  - a unique name
 *  - a list of parameters
 *  - type safe input task results
 *  - an output type
 *
 * Task instances have (TI)
 *  - a TD
 *  - specific values for all TD parameters
 *  - a list of lazy input TI
 *  - code for producing the output
 *
 * Notes
 *  - creating task instances should only yield a full dependency tree based on the task parameters
 *  - inputs to tasks are instantiated lazily so a partial graph could be examined
 *  - execution of tasks is subject to execution control and memoization
 *
 *
 * other ideas (todo)
 *  - reader-monad-like 'ask' dependencies
 *    - input tasks that are pushed down to the bottom of the execution graph (before everything)
 *  - facts matching as basis for dependency satisfaction
 */
public class Scratch {

  public static void main(String[] args) throws IOException {
    Task<Exec.Result> foo = exec("foobar", 123);
    foo.inputsInOrder()
        .map(Task::id)
        .forEachOrdered(System.out::println);

    TaskInfo taskInfo = TaskInfo.ofTask(foo);
    ObjectMapper objectMapper = new ObjectMapper()
      .enable(SerializationFeature.INDENT_OUTPUT);
    String json = objectMapper.writeValueAsString(taskInfo);
    System.out.println(json);
  }

  @RootTask
  static Task<Exec.Result> exec(String parameter, int number) {
    Task<String> task1 = MyTask.create(parameter);
    Task<Integer> task2 = Adder.create(number, number + 2);

    return Task.named("exec", "/bin/sh")
        .in(() -> task1)
        .in(() -> task2)
        .process(Exec.exec((str, i) -> args("/bin/sh", "-c", "\"echo " + i + "\"")));
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

    static String something(String parameter, int sum, long fib) {
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
