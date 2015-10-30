package failing;

import io.rouz.task.Task;
import io.rouz.task.processor.RootTask;

class ProtectedMethod {

  @RootTask
  protected static Task<?> privateMethod() {
    return null;
  }
}
