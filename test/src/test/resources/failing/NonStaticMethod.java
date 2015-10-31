package failing;

import io.rouz.task.Task;
import io.rouz.task.processor.RootTask;

class NonStaticMethod {

  @RootTask
  Task<?> instanceMethod() {
    return null;
  }
}
