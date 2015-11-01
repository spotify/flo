package io.rouz.task.cli;

import io.rouz.task.Task;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import joptsimple.NonOptionArgumentSpec;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import joptsimple.OptionSpecBuilder;

import static java.lang.System.out;
import static java.util.Arrays.asList;

/**
 * TODO: document.
 */
public final class Cli {

  private final List<Function<Map<String, String>, Task<?>>> factories;

  private Cli(List<Function<Map<String, String>, Task<?>>> factories) {
    this.factories = factories;
  }

  @SafeVarargs
  public static Cli forFactories(Function<Map<String, String>, Task<?>>... factories) {
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
    for (Function<Map<String, String>, Task<?>> factory : factories) {
//      out.println(verbose ? task.id() : task.id().name());
      out.println(factory);
    }
  }

  private void create(List<String> nonOptions) {
    final OptionParser optionParser = secondaryParser(nonOptions);
    final OptionSet parse = optionParser.parse(nonOptions.toArray(new String[nonOptions.size()]));

    final Map<String, String> args = new LinkedHashMap<>();
    for (String option : optionParser.recognizedOptions().keySet()) {
      if ("[arguments]".equals(option)) {
        continue;
      }
      args.put(option, parse.valueOf(option).toString());
    }

    out.println("nonOptions = " + nonOptions);
    out.println("optionParser.recognizedOptions() = " + optionParser.recognizedOptions());
    out.println("parse.asMap() = " + parse.asMap());
    out.println("args = " + args);
    out.println("creating tasks:\n");
    for (Function<Map<String, String>, Task<?>> factory : factories) {
      final Task<?> createdTask = factory.apply(args);
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

  private static OptionParser secondaryParser(List<String> remainingArgs) {
    final OptionParser parser = new OptionParser();
    final int len = remainingArgs.size();

    for (int i = 0; i < len; i++) {
      final String arg = remainingArgs.get(i);
      if (isOpt(arg)) {
        final String option = arg.substring(2);
        final OptionSpecBuilder spec = parser.accepts(option);
        final boolean isFlag = i + 1 == len || isOpt(remainingArgs.get(i + 1));

        if (isFlag) {
          spec.withOptionalArg().ofType(boolean.class).defaultsTo(true);
        } else {
          spec.withOptionalArg();
        }
      }
    }

    return parser;
  }

  private static boolean isOpt(String string) {
    return string.startsWith("--");
  }
}
