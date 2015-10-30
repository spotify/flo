package io.rouz.task.processor;

import com.google.testing.compile.JavaFileObjects;

import org.junit.Test;

import javax.tools.JavaFileObject;

import static com.google.common.truth.Truth.assert_;
import static com.google.testing.compile.JavaSourceSubjectFactory.javaSource;

public class TaskBindingProcessorTest {

  @Test
  public void failOnMethodNotReturningTask() {
    JavaFileObject source = JavaFileObjects.forResource("failing/MethodNotReturningTask.java");
    assert_().about(javaSource())
        .that(source)
        .processedWith(new TaskBindingProcessor())
        .failsToCompile()
        .withErrorContaining("method does not return a io.rouz.task.Task<?>");
  }
}
