package io.rouz.flo;

/**
 * Provider interface for operation objects that will be injected into tasks
 */
public interface OpProvider<T> {

  T provide(TaskContext taskContext);

  default void preRun(Task<?> task) {
  }

  default void onSuccess(Task<?> task, Object z) {
  }

  default void onFail(Task<?> task, Throwable throwable) {
  }

  default TaskContext.Value<T> provideAsync(TaskContext taskContext) {
    return taskContext.value(() -> provide(taskContext));
  }
}
