package failing;

import com.spotify.flo.Task;
import com.spotify.flo.processor.RootTask;
import java.util.List;

class UnsupportedArgTask {

  @RootTask
  static Task<String> notTask(List unsupported) {
    return null;
  }
}
