package io.rouz.flo.freezer;

import static io.rouz.flo.Util.colored;

import io.rouz.flo.TaskId;
import io.rouz.flo.context.InstrumentedContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link InstrumentedContext.Listener} that prints to an slf4j Logger.
 */
public class LoggingListener implements InstrumentedContext.Listener {

  private static final Logger LOG = LoggerFactory.getLogger(LoggingListener.class);

  @Override
  public void edge(TaskId upstream, TaskId downstream) {
    LOG.info("{} <- {}", colored(upstream), colored(downstream));
  }

  @Override
  public void status(TaskId task, Phase phase) {
    LOG.info("{} :: {}", colored(task), phase);
  }
}
