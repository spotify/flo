package compiling;

import io.rouz.task.Task;
import io.rouz.task.processor.RootTask;

class PlainTaskConstructor {

  @RootTask
  static Task<String> simple() {
    return null;
  }
}
