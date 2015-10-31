package failing;

import io.rouz.task.Task;
import io.rouz.task.processor.RootTask;

class PrivateMethod {

  @RootTask
  private static Task<?> privateMethod() {
    return null;
  }
}
