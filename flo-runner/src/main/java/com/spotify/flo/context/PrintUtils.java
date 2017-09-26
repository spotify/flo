/*-
 * -\-\-
 * flo runner
 * --
 * Copyright (C) 2016 Spotify AB
 * --
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * -/-/-
 */

package com.spotify.flo.context;

import static java.util.stream.Collectors.joining;
import static org.fusesource.jansi.Ansi.Color.CYAN;
import static org.fusesource.jansi.Ansi.Color.WHITE;
import static org.fusesource.jansi.Ansi.Color.YELLOW;
import static org.fusesource.jansi.Ansi.ansi;

import com.google.common.collect.ImmutableList;
import com.spotify.flo.TaskId;
import com.spotify.flo.TaskInfo;
import java.util.ArrayList;
import java.util.List;
import java.util.Stack;
import org.fusesource.jansi.Ansi;

/**
 * Utilities for printing the task tree
 */
final class PrintUtils {

  private PrintUtils() {
  }

  static Ansi colored(Ansi.Color color, String string) {
    return ansi().fg(color).a(string).reset();
  }

  static Ansi colored(TaskId taskId) {
    final String id = taskId.toString();
    final int openParen = id.indexOf('(');
    final int closeParen = id.lastIndexOf(')');
    final int hashPos = id.lastIndexOf('#');

    return ansi()
        .fg(CYAN).a(id.substring(0, openParen + 1))
        .reset().a(id.substring(openParen + 1, closeParen))
        .fg(CYAN).a(id.substring(closeParen, hashPos))
        .fg(WHITE).a(id.substring(hashPos))
        .reset();
  }

  static List<String> tree(TaskInfo taskInfo) {
    final List<String> lines = new ArrayList<>();

    lines.add(colored(taskInfo.id()).toString());
    popSubTree(taskInfo.inputs(), lines, new Stack<>());

    return ImmutableList.copyOf(lines);
  }

  private static void popSubTree(List<TaskInfo> inputs, List<String> list, Stack<Boolean> indents) {
    for (int i = 0; i < inputs.size(); i++) {
      final TaskInfo taskInfo = inputs.get(i);
      final String indent = indents.stream()
          .map(b -> b ? "   " : "│  ")
          .map(s -> ansi().fg(Ansi.Color.WHITE).a(s).toString())
          .collect(joining());
      final String branch = (i < inputs.size() - 1) ? "├" : "└";
      final String prefix = indent + ansi().fg(Ansi.Color.WHITE).a(branch + "▸ ").toString();
      final String refArrow = taskInfo.isReference()
          ? ansi().fg(YELLOW).a(" ⤴").reset().toString()
          : "";

      list.add(prefix + colored(taskInfo.id()) + refArrow);

      indents.push(i == inputs.size() - 1);
      popSubTree(taskInfo.inputs(), list, indents);
      indents.pop();
    }
  }
}
