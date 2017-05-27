package io.rouz.flo;

import static org.fusesource.jansi.Ansi.Color.CYAN;
import static org.fusesource.jansi.Ansi.Color.WHITE;
import static org.fusesource.jansi.Ansi.ansi;

import org.fusesource.jansi.Ansi;

public final class Util {

  private Util() {
  }

  public static Ansi colored(TaskId taskId) {
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
}
