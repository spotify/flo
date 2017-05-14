package io.rouz.flo.freezer;

import static org.fusesource.jansi.Ansi.Color.CYAN;
import static org.fusesource.jansi.Ansi.Color.WHITE;
import static org.fusesource.jansi.Ansi.ansi;

import io.rouz.flo.TaskId;
import io.rouz.flo.context.InstrumentedContext;
import org.fusesource.jansi.Ansi;
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

  public static Ansi colored(TaskId taskId) {
    final String id = taskId.toString();
    final int openParen = id.indexOf('(');
    final int closeParen = id.lastIndexOf(')');
    final int hashPos = id.lastIndexOf('#');

    return ansi()
        .fg(CYAN).a(id.substring(0, openParen + 1))
        .reset().a(id.substring(openParen + 1, closeParen))
        .fg(CYAN).a(id.substring(closeParen, hashPos))
        .fg(WHITE).a(id.substring(hashPos))
        .reset();
  }
}
