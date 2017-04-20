package failing;

import io.rouz.flo.Task;
import io.rouz.flo.processor.RootTask;
import java.util.List;

class UnsupportedArgTask {

  @RootTask
  static Task<String> notTask(List unsupported) {
    return null;
  }
}
