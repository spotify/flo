package io.rouz.scratch;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import io.rouz.flo.Task;
import io.rouz.flo.TaskContext;
import io.rouz.flo.TaskId;
import io.rouz.flo.TaskInfo;
import io.rouz.flo.context.AwaitingConsumer;
import io.rouz.flo.context.InstrumentedContext;
import io.rouz.flo.context.MemoizingContext;
import io.rouz.flo.proc.Exec;
import io.rouz.flo.processor.RootTask;

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

  public static void main(String[] args) throws Exception {
    Task<Exec.Result> foo = exec("foobar", 123);
    foo.inputsInOrder()
        .map(Task::id)
        .forEachOrdered(System.out::println);

    TaskInfo taskInfo = TaskInfo.ofTask(foo);
    ObjectMapper objectMapper = new ObjectMapper()
        .enable(SerializationFeature.INDENT_OUTPUT);
    String json = objectMapper.writeValueAsString(taskInfo);
    System.out.println(json);

    AwaitingConsumer<Exec.Result> result = AwaitingConsumer.create();
    TaskContext ctx = MemoizingContext.composeWith(
        InstrumentedContext.composeWith(
            TaskContext.inmem(), new L()));
    ctx.evaluate(foo).consume(result);
    result.awaitAndGet();
  }

  static class L implements InstrumentedContext.Listener {

    @Override
    public void edge(TaskId upstream, TaskId downstream) {
      System.out.println(upstream + " <- " + downstream);
    }

    @Override
    public void status(TaskId task, Phase phase) {
      System.out.println(task + " :: " + phase);
    }
  }

  @RootTask
  static Task<Exec.Result> exec(String parameter, int number) {
    Task<String> task1 = MyTask.create(parameter);
    Task<Integer> task2 = Adder.create(number, number + 2);

    return Task.named("exec", "/bin/sh").ofType(Exec.Result.class)
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
      return Task.named("MyTask", parameter).ofType(String.class)
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
      return Task.named("Adder", a, b).ofType(Integer.class).process(() -> a + b);
    }
  }
}
