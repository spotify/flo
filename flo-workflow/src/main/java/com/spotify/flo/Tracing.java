package com.spotify.flo;

import io.grpc.Context;

public class Tracing {

  public static final Context.Key<String> TASK_NAME = Context.keyWithDefault("task-name", "");

}
