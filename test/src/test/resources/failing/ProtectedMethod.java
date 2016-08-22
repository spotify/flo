package failing;

import io.rouz.flo.Task;
import io.rouz.flo.processor.RootTask;

class ProtectedMethod {

  @RootTask
  protected static Task<?> privateMethod() {
    return null;
  }
}
