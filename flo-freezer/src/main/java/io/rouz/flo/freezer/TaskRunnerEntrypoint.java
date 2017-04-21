package io.rouz.flo.freezer;

import io.rouz.flo.TaskContext;
import io.rouz.flo.context.AwaitingConsumer;
import io.rouz.flo.context.InstrumentedContext;
import io.rouz.flo.context.MemoizingContext;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * TODO: document.
 */
public class TaskRunnerEntrypoint {

  private static final Logger LOG = LoggerFactory.getLogger(TaskRunnerEntrypoint.class);

  public static void main(String[] args) throws InterruptedException {
    if (args.length < 1) {
      LOG.info("Usage: flo-task-runner <persisted-task-file>");
      System.exit(1);
    }

    final String file = args[0];
    final Path filePath = Paths.get(file);

    final EvaluatingContext evaluatingContext = new EvaluatingContext(
        filePath.resolveSibling(""), MemoizingContext.composeWith(
            InstrumentedContext.composeWith(
                TaskContext.inmem(), new LoggingListener())));

    final AwaitingConsumer < Object > res = AwaitingConsumer.create();
    final TaskContext.Value<Object> value = evaluatingContext.evaluateTaskFrom(filePath);
    value.consume(res);
    value.onFail(Throwable::printStackTrace);

    res.await(5, TimeUnit.SECONDS);
    final Object output = res.get();

    LOG.info("res.get() = " + output);
  }
}
