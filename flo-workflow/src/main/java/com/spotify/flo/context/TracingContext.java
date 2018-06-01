package com.spotify.flo.context;

import static com.spotify.flo.Tracing.TASK_ARGS;
import static com.spotify.flo.Tracing.TASK_ID;
import static com.spotify.flo.Tracing.TASK_NAME;

import com.spotify.flo.EvalContext;
import com.spotify.flo.Fn;
import com.spotify.flo.TaskId;
import io.grpc.Context;

public class TracingContext extends ForwardingEvalContext {

  private TracingContext(EvalContext delegate) {
    super(delegate);
  }

  public static EvalContext composeWith(EvalContext baseContext) {
    return new TracingContext(baseContext);
  }

  @Override
  public <T> Value<T> invokeProcessFn(TaskId taskId, Fn<Value<T>> processFn) {
    try {
      return Context.current()
          .withValue(TASK_ID, taskId.toString())
          .withValue(TASK_NAME, taskId.name())
          .withValue(TASK_ARGS, taskId.args())
          .call(() -> super.invokeProcessFn(taskId, processFn));
    } catch (Exception e) {
      if (e instanceof RuntimeException) {
        throw (RuntimeException) e;
      } else {
        throw new RuntimeException(e);
      }
    }
  }
}
