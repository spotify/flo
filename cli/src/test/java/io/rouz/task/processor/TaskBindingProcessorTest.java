package io.rouz.task.processor;

import org.junit.Test;

import javax.tools.JavaFileObject;

import static com.google.common.truth.Truth.assert_;
import static com.google.testing.compile.JavaFileObjects.forResource;
import static com.google.testing.compile.JavaSourceSubjectFactory.javaSource;

public class TaskBindingProcessorTest {

  TaskBindingProcessor processor = new TaskBindingProcessor();

  @Test
  public void failOnNonStaticMethod() {
    JavaFileObject source = forResource("failing/NonStaticMethod.java");
    assert_().about(javaSource())
        .that(source)
        .processedWith(processor)
        .failsToCompile()
        .withErrorContaining("annotated method must be static");
  }

  @Test
  public void failOnPrivateMethod() {
    JavaFileObject source = forResource("failing/PrivateMethod.java");
    assert_().about(javaSource())
        .that(source)
        .processedWith(processor)
        .failsToCompile()
        .withErrorContaining("annotated method must not be private");
  }

  @Test
  public void failOnProtectedMethod() {
    JavaFileObject source = forResource("failing/ProtectedMethod.java");
    assert_().about(javaSource())
        .that(source)
        .processedWith(processor)
        .failsToCompile()
        .withErrorContaining("annotated method must not be protected");
  }

  @Test
  public void failOnMethodNotReturningTask() {
    JavaFileObject source = forResource("failing/MethodNotReturningTask.java");
    assert_().about(javaSource())
        .that(source)
        .processedWith(processor)
        .failsToCompile()
        .withErrorContaining("annotated method must return a io.rouz.task.Task<?>");
  }
}
