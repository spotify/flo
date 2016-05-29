package io.rouz.flo.gen;

import org.junit.Test;

import javax.tools.JavaFileObject;

import static com.google.common.truth.Truth.assert_;
import static com.google.testing.compile.JavaFileObjects.forResource;
import static com.google.testing.compile.JavaSourceSubjectFactory.javaSource;

public class ApiGeneratorProcessorTest {

  ApiGeneratorProcessor processor = new ApiGeneratorProcessor();

  @Test
  public void shouldCompilePlainTaskBinding() {
    JavaFileObject source = forResource("source/GeneratedApiTemplate.java");
    assert_().about(javaSource())
        .that(source)
        .processedWith(processor)
        .compilesWithoutError()
        .and()
        .generatesSources(forResource("generated/GeneratedApi.java"));
  }
}
