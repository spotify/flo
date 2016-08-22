package compiling;

import io.rouz.flo.Task;
import io.rouz.flo.processor.RootTask;

class PlainTaskConstructor {

  @RootTask
  static Task<String> simple() {
    return null;
  }
}
