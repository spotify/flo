package common.pkg.sibling2;

import com.spotify.flo.Task;
import com.spotify.flo.processor.RootTask;

public class Sibling2 {

  @RootTask
  public static Task<String> simple2() {
    return null;
  }
}
