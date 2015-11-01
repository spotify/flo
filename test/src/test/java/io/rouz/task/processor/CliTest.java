package io.rouz.task.processor;

import io.rouz.task.Task;
import io.rouz.task.cli.Cli;

import org.junit.Test;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

public class CliTest {

  private static int firstInt;
  private static String firstString;
  private static String secondString;

  private String[] args = {
      "create", "--first", "22", "--flag1", "--second", "hello", "--flag2"};

  @Test
  public void testName() throws Exception {
    Cli.forFactories(FloRootTaskFactory::testTask1, FloRootTaskFactory::testTask2)
        .run(args);

    assertThat(firstInt, is(22));
    assertThat(firstString, is("22"));
    assertThat(secondString, is("hello"));
  }

  @RootTask
  public static Task<String> testTask1(int first, String second) {
    firstInt = first;
    secondString = second;
    return Task.named("Test1", first, second)
        .process(() -> second + " " + first * 100);
  }

  @RootTask
  public static Task<String> testTask2(String first) {
    firstString = first;
    return Task.named("Test2", first)
        .process(() -> first + " as String");
  }
}
