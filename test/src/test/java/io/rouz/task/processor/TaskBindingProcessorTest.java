package io.rouz.task.processor;

import org.junit.Test;

import java.util.Arrays;

import javax.tools.JavaFileObject;

import static com.google.common.truth.Truth.assert_;
import static com.google.testing.compile.JavaFileObjects.forResource;
import static com.google.testing.compile.JavaSourceSubjectFactory.javaSource;
import static com.google.testing.compile.JavaSourcesSubjectFactory.javaSources;

public class TaskBindingProcessorTest {

  TaskBindingProcessor processor = new TaskBindingProcessor();

  @Test
  public void shouldCompilePlainTaskBinding() {
    JavaFileObject source = forResource("compiling/PlainTaskConstructor.java");
    assert_().about(javaSource())
        .that(source)
        .processedWith(processor)
        .compilesWithoutError()
        .and()
        .generatesSources(forResource("out/PlainTaskConstructorFactory.java"));
  }

  @Test
  public void shouldCompileArgsTaskBinding() {
    JavaFileObject source = forResource("compiling/ArgsTaskConstructor.java");
    assert_().about(javaSource())
        .that(source)
        .processedWith(processor)
        .compilesWithoutError()
        .and()
        .generatesSources(forResource("out/ArgsTaskConstructorFactory.java"));
  }

  @Test
  public void shouldPutFactoryInCommonPackage() throws Exception {
    JavaFileObject source1 = forResource("compiling/Sibling1.java");
    JavaFileObject source2 = forResource("compiling/Sibling2.java");
    assert_().about(javaSources())
        .that(Arrays.asList(source1, source2))
        .processedWith(processor)
        .compilesWithoutError()
        .and()
        .generatesSources(forResource("out/SiblingCommonPackage.java"));
  }

  @Test
  public void failOnUnsupportedArgType() {
    JavaFileObject source = forResource("failing/UnsupportedArgTask.java");
    assert_().about(javaSource())
        .that(source)
        .processedWith(processor)
        .failsToCompile()
        .withErrorContaining("Unsupported argument type for @RootTask annotation: java.util.List");
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
}
