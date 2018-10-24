package com.spotify.flo.context;

import com.spotify.flo.TaskInfo;
import java.util.List;

public class PrintUtilsBridge {
  public static List<String> tree(TaskInfo taskInfo) {
    return PrintUtils.tree(taskInfo);
  }
}
