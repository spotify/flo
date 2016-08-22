package failing;

import io.rouz.flo.Task;
import io.rouz.flo.processor.RootTask;

class NonStaticMethod {

  @RootTask
  Task<?> instanceMethod() {
    return null;
  }
}
