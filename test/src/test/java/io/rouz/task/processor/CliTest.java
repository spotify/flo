package io.rouz.task.processor;

import io.rouz.task.Task;
import io.rouz.task.cli.Cli;

import org.junit.Test;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class CliTest {

  private static int firstInt;
  private static String secondString;
  private static boolean firstFlag = false;
  private static boolean secondFlag = true;

  private String[] args = {
      "create", "--first", "22", "--flag1", "--second", "hello"};

  @Test
  public void testName() throws Exception {
    Cli.forFactories(FloRootTaskFactory.CliTest_TestTask())
        .run(args);

    assertThat(firstInt, is(22));
    assertThat(secondString, is("hello"));
    assertTrue(firstFlag); // toggled
    assertFalse(secondFlag);  // toggled
  }

  @RootTask
  public static Task<String> testTask(int first, boolean flag1, String second, boolean flag2) {
    firstInt = first;
    secondString = second;
    firstFlag = flag1;
    secondFlag = flag2;
    return Task.named("Test1", first, second)
        .process(() -> second + " " + first * 100);
  }
}
