package com.spotify.flo;

import io.grpc.Context;

public final class Tracing {

  public static final Context.Key<String> TASK_ID = Context.keyWithDefault("task-id", "");
  public static final Context.Key<String> TASK_NAME = Context.keyWithDefault("task-name", "");
  public static final Context.Key<String> TASK_ARGS = Context.keyWithDefault("task-args", "");

  private Tracing() {
    throw new UnsupportedOperationException();
  }
}
