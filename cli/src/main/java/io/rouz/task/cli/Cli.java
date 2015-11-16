package io.rouz.task.cli;

import io.rouz.task.Task;

import java.io.IOException;
import java.util.List;

import joptsimple.NonOptionArgumentSpec;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;

import static java.lang.System.out;
import static java.util.Arrays.asList;

/**
 * TODO: document.
 */
public final class Cli {

  private final List<TaskConstructor<?>> factories;

  private Cli(List<TaskConstructor<?>> factories) {
    this.factories = factories;
  }

  @SafeVarargs
  public static Cli forFactories(TaskConstructor<?>... factories) {
    return new Cli(asList(factories));
  }

  public void run(String... args) throws IOException {
    final OptionParser parser = new OptionParser();
    final OptionSpec<Void> h = parser.acceptsAll(asList("h", "help"), "Print usage info").forHelp();

    final NonOptionArgumentSpec<String> nonOptions = parser.nonOptions();
    parser.allowsUnrecognizedOptions();

    final OptionSet parse = parser.parse(args);
    if (parse.has(h)) {
      out.println("Flo 0.0.1");
      usage(parser);
    }
    if (parse.nonOptionArguments().size() < 1) {
      out.println("Too few arguments");
      usage(parser);
    }

    final String command = parse.valuesOf(nonOptions).get(0);
    switch (command) {
      case "list":
        list();
        break;

      case "create":
        create(parse.valuesOf(nonOptions));
        break;

      default:
        out.println("'" + command + "' is not a command");
        usage(parser);
    }
  }

  private void list() {
    out.println("available tasks:\n");
    for (TaskConstructor<?> factory : factories) {
      out.println(factory.name());
    }
  }

  private void create(List<String> nonOptions) {
    final String[] args = nonOptions.toArray(new String[nonOptions.size()]);

    out.println("nonOptions = " + nonOptions);
    out.println("creating tasks:\n");
    for (TaskConstructor<?> factory : factories) {
      final Task<?> createdTask = factory.create(args);
      out.println("createdTask.id() = " + createdTask.id());
      out.println("createdTask.out() = " + createdTask.out());
    }
  }

  private static void usage(OptionParser parser) throws IOException {
    out.println("  usage: flo <command> [<options>]\n");
    out.println("Commands:\n  list\n");
    parser.printHelpOn(out);
    System.exit(1);
  }
}
