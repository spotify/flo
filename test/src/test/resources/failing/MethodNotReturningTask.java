package failing;

import io.rouz.flo.processor.RootTask;

class MethodNotReturningTask {

  @RootTask
  static String notTask() {
    return "I should be a Task<?>";
  }
}
