package com.spotify.flo.context;

import static org.mockito.Mockito.verify;

import com.google.common.collect.ImmutableList;
import com.spotify.flo.TaskId;
import com.spotify.flo.TaskInfo;
import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.slf4j.Logger;

@RunWith(MockitoJUnitRunner.class)
public class LoggingTest {

  private static final TaskId FOO = TaskId.create("foo");
  private static final TaskId BAR = TaskId.create("bar");
  private static final TaskId BAZ = TaskId.create("baz");
  private static final TaskInfo TASK_INFO = TaskInfo.create(FOO, ImmutableList.of(
      TaskInfo.create(BAR, Collections.emptyList()),
      TaskInfo.create(BAZ, Collections.emptyList())));

  @Mock private Logger logger;

  private Logging sut;

  @Before
  public void setUp() {
    sut = Logging.create(logger);
  }

  @Test
  public void completeShouldLogSummary() {
    sut.completedValue(BAR, "bar-value", Duration.ofSeconds(3));
    sut.failedValue(BAZ, new IOException("bug!"), Duration.ofSeconds(4));
    sut.complete(TASK_INFO, Duration.ofSeconds(17));

    verify(logger).info("Total time {}", "00:00:17.000");
    verify(logger).info("Executed {} out of {} tasks:", 2, 3);

    verify(logger).info("{}: {}", "" + FOO, "Pending");
    verify(logger).info("{}: {}", "├▸ " + BAR, "Success");
    verify(logger).info("{}: {}", "└▸ " + BAZ, "Failure: java.io.IOException: bug!");
  }
}