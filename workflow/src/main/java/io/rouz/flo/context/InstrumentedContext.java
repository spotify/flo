package io.rouz.flo.context;

import io.rouz.flo.Fn;
import io.rouz.flo.Task;
import io.rouz.flo.TaskContext;
import io.rouz.flo.TaskId;
import java.util.Objects;

/**
 * A {@link TaskContext} that instruments the task expansion and invocation process.
 *
 * <p>This context will invoke methods on a {@link Listener} when tasks are evaluated through the
 * {@link #evaluate(Task)} method.
 *
 * <p>The {@link Listener#edge(TaskId, TaskId)} method is called for each discovered
 * upstream-downstream relation between two tasks that are about to be evaluated. This call is
 * always made before the upstream is evaluated.
 *
 * <p>The {@link Listener#status(TaskId, Listener.Phase)} method is called when a task is actually
 * being processed, i.e. when the {@link #invokeProcessFn(TaskId, Fn)} of that task in called in
 * the {@link TaskContext}. There will be at most two calls made for
 * each task. First with {@link Listener.Phase#START}, when evaluation starts. Then with either
 * {@link Listener.Phase#SUCCESS} or {@link Listener.Phase#FAILURE} depending on the success or
 * failure of the task {@link TaskContext.Value}.
 *
 */
public class InstrumentedContext extends ForwardingTaskContext {

  /**
   * A listener for instrumented evaluation. See {@link InstrumentedContext} for more details.
   */
  public interface Listener {

    /**
     * Called when an edge (dependency) between two tasks are discovered. The relation is that
     * the downstream task depends on the upstream task. Also that the upstream task has to be
     * evaluated first, before the downstream task can be evaluated.
     *
     * @param upstream    The id of the upstream task
     * @param downstream  The id of the downstream task
     */
    void edge(TaskId upstream, TaskId downstream);

    /**
     * Called when a task starts and finished it's evaluation. This will be called exactly twice
     * per evaluation of a task.
     *
     * @param task   The task that is being evaluated
     * @param phase  The phase of evaluation
     */
    void status(TaskId task, Phase phase);

    /**
     * The different phases of task evaluation.
     */
    enum Phase {
      /**
       * The task has started evaluating
       */
      START,

      /**
       * The task completed evaluating successfully
       */
      SUCCESS,

      /**
       * The task completed evaluating with a failure
       */
      FAILURE
    }
  }

  private final Listener listener;

  private InstrumentedContext(TaskContext baseContext, Listener listener) {
    super(baseContext);
    this.listener = Objects.requireNonNull(listener);
  }

  public static TaskContext composeWith(TaskContext baseContext, Listener listener) {
    return new InstrumentedContext(baseContext, listener);
  }

  @Override
  public <T> Value<T> evaluateInternal(Task<T> task, TaskContext context) {
    task.inputs().stream()
        .map(Task::id)
        .distinct()
        .forEach(upstream -> listener.edge(upstream, task.id()));

    return delegate.evaluateInternal(task, context);
  }

  @Override
  public <T> Value<T> invokeProcessFn(TaskId taskId, Fn<Value<T>> processFn) {
    listener.status(taskId, Listener.Phase.START);

    final Value<T> tValue = delegate.invokeProcessFn(taskId, processFn);

    tValue.consume(v -> listener.status(taskId, Listener.Phase.SUCCESS));
    tValue.onFail(t -> listener.status(taskId, Listener.Phase.FAILURE));

    return tValue;
  }
}
