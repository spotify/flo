package com.spotify.flo;

import io.vavr.Tuple2;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class MultiTaskOutputTest {

  @Mock TaskOutput<String, String> o1;
  @Mock TaskOutput<Integer, Integer> o2;

  @Test
  public void multiOutputTask() {

    final Task<Tuple2<String, Integer>> foo = Task.named("foo")
        .ofType(String.class, Integer.class)
        .output(MultiTaskOutput.of(o1, o2))
        .process((Tuple2<String, Integer> p) -> {
          return p;
        });
  }
}