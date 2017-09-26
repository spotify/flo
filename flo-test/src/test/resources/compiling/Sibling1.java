package common.pkg.sibling1;

import com.spotify.flo.Task;
import com.spotify.flo.processor.RootTask;

public class Sibling1 {

  @RootTask
  public static Task<String> simple1() {
    return null;
  }
}
