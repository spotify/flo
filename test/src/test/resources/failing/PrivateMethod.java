package failing;

import io.rouz.flo.Task;
import io.rouz.flo.processor.RootTask;

class PrivateMethod {

  @RootTask
  private static Task<?> privateMethod() {
    return null;
  }
}
