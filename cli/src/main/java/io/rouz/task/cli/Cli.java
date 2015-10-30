package io.rouz.task.cli;

import io.rouz.task.Task;

import java.io.IOException;
import java.util.List;

import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;

import static java.lang.System.out;
import static java.util.Arrays.asList;

/**
 * TODO: document.
 */
public final class Cli {

  public static void run(List<Task<?>> tasks, String... args) throws IOException {
    final OptionParser parser = new OptionParser();
    final OptionSpec<Void> v = parser.acceptsAll(asList("v", "verbose"), "Verbose output");

    parser.nonOptions().isRequired();

    final OptionSet parse = parser.parse(args);
    if (parse.nonOptionArguments().size() < 1) {
      out.println("Too few arguments");
      usage(parser);
    }

    final boolean verbose = parse.has(v);

    final String command = parse.nonOptionArguments().get(0).toString();
    switch (command) {
      case "list":
        list(tasks, verbose);
        break;

      default:
        out.println("'" + command + "' is not a command");
        usage(parser);
    }
  }

  private static void list(List<Task<?>> tasks, boolean verbose) {
    out.println("available tasks:\n");
    for (Task<?> task : tasks) {
      out.println(verbose ? task.id() : task.id().name());
    }
  }

  private static void usage(OptionParser parser) throws IOException {
    out.println("  usage: flo <command> [<options>]\n");
    out.println("Commands:\n  list\n");
    parser.printHelpOn(out);
    System.exit(1);
  }
}
