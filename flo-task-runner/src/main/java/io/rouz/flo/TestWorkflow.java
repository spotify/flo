package io.rouz.flo;

import static io.rouz.flo.TaskContext.inmem;
import static io.rouz.flo.freezer.LoggingListener.colored;

import io.rouz.flo.TaskContext.Value;
import io.rouz.flo.context.AwaitingConsumer;
import io.rouz.flo.context.InstrumentedContext;
import io.rouz.flo.context.MemoizingContext;
import io.rouz.flo.freezer.LoggingListener;
import io.rouz.flo.freezer.PersistingContext;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestWorkflow {

  private static final Logger LOG = LoggerFactory.getLogger(TestWorkflow.class);

  public static void main(String[] args) throws InterruptedException {
    Task<Long> fib9 = fib(9);
    persist(fib9);
  }

  public static Task<Long> fib(long n) {
    TaskBuilder<Long> fib = Task.named("Fib", n).ofType(Long.class);
    if (n < 2) {
      return fib
          .process(() -> n);
    } else {
      return fib
          .in(() -> fib(n - 1))
          .in(() -> fib(n - 2))
          .process(TestWorkflow::fib);
    }
  }

  static long fib(long a, long b) {
    LOG.info("Fib.process(" + a + " + " + b + ") = " + (a + b));
    return a + b;
  }

  private static void persist(Task<?> task) throws InterruptedException {
    final String cwd = System.getProperty("user.dir");
    final Path basePath = Paths.get(cwd).resolve("run-" + randomAlphaNumeric(4));

    try {
      Files.createDirectories(basePath);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    PersistingContext persistingContext = new PersistingContext(basePath, inmem());
    TaskContext context = MemoizingContext.composeWith(
        InstrumentedContext.composeWith(
            persistingContext, new LoggingListener()));

    AwaitingConsumer<Throwable> await = AwaitingConsumer.create();
    Value<?> value = context.evaluate(task);
    value.onFail(await);

    await.await(5, TimeUnit.SECONDS);
    Map<TaskId, Path> files = persistingContext.getFiles();
    files.forEach((taskId, file) ->
        LOG.info("{} -> {}", colored(taskId), file)
    );
  }

  private static final String ALPHA_NUMERIC_STRING = "abcdefghijklmnopqrstuvwxyz0123456789";

  public static String randomAlphaNumeric(int count) {
    StringBuilder builder = new StringBuilder();
    while (count-- != 0) {
      int character = (int)(Math.random() * ALPHA_NUMERIC_STRING.length());
      builder.append(ALPHA_NUMERIC_STRING.charAt(character));
    }
    return builder.toString();
  }
}
