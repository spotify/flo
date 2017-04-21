package io.rouz.flo.freezer;

import static io.rouz.flo.freezer.LoggingListener.colored;
import static io.rouz.flo.freezer.PersistingContext.cleanForFilename;

import io.rouz.flo.Fn;
import io.rouz.flo.Task;
import io.rouz.flo.TaskContext;
import io.rouz.flo.TaskId;
import io.rouz.flo.context.ForwardingTaskContext;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * TODO: document.
 */
public class EvaluatingContext {

  static final String OUTPUT_SUFFIX = "_out";

  private final Path basePath;
  private final TaskContext delegate;

  public EvaluatingContext(Path basePath, TaskContext delegate) {
    this.basePath = Objects.requireNonNull(basePath);
    this.delegate = Objects.requireNonNull(delegate);
  }

  public <T> TaskContext.Value<T> evaluateTaskFrom(Path persistedTask) {
    Task<T> task = null;
    try {
      task = PersistingContext.deserialize(persistedTask);
    } catch (Exception e) {
      e.printStackTrace();
    }

    return new SpecificEval(task, delegate).evaluate(task);
  }

  private Path resolveExistingOutput(TaskId taskId) {
    final String fileName = cleanForFilename(taskId) + OUTPUT_SUFFIX;
    return basePath.resolve(fileName);
  }

  private <T> void persist(TaskId taskId, T output) {
    final Path outputPath = basePath.resolve(cleanForFilename(taskId) + OUTPUT_SUFFIX);

    try {
      PersistingContext.serialize(output, outputPath);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private class SpecificEval extends ForwardingTaskContext {

    private final Task<?> evalTask;

    protected SpecificEval(Task<?> evalTask, TaskContext delegate) {
      super(delegate);
      this.evalTask = evalTask;
    }

    @Override
    public <T> Value<T> evaluateInternal(Task<T> task, TaskContext context) {
      final Promise<T> promise = promise();
      final TaskId id = task.id();
      final Set<TaskId> inputTaskIds = evalTask.inputs().stream()
          .map(Task::id)
          .collect(Collectors.toSet());

      if (inputTaskIds.contains(id)) {
        final Path inputValuePath = resolveExistingOutput(id);
        if (Files.exists(inputValuePath)) {
          final T value;
          try {
            value = PersistingContext.deserialize(inputValuePath);
            promise.set(value);
          } catch (Exception e) {
            promise.fail(e);
          }
        } else {
          promise.fail(new RuntimeException("Output value for input task " + id + " not found"));
        }
      } else if (!id.equals(evalTask.id())) {
        promise.fail(new RuntimeException("Evaluation of unexpected task: " + id));
      } else {
        final Value<T> tValue = super.evaluateInternal(task, context);
        tValue.consume(v -> persist(evalTask.id(), v));
        tValue.consume(promise::set);
        tValue.onFail(promise::fail);
      }

      return promise.value();
    }

    @Override
    public <T> Value<T> invokeProcessFn(TaskId taskId, Fn<Value<T>> processFn) {
      // todo: only invoke fn if taskId == evalTask
      // todo: fail if called for taskId != evalTask

      final Value<T> tValue = super.invokeProcessFn(taskId, processFn);
      tValue.consume(v -> LOG.info("{} == {}", colored(taskId), v));
      return tValue;
    }
  }
}
