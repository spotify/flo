package failing;

import io.rouz.task.Task;
import io.rouz.task.processor.RootTask;

import java.util.List;

class UnsupportedArgTask {

  @RootTask
  static Task<String> notTask(List unsupported) {
    return null;
  }
}
