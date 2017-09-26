package failing;

import com.spotify.flo.Task;
import com.spotify.flo.processor.RootTask;

class PrivateMethod {

  @RootTask
  private static Task<?> privateMethod() {
    return null;
  }
}
