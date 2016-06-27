package io.rouz.flo.gen;

import com.google.common.truth.Truth;
import com.google.testing.compile.JavaFileObjects;
import com.google.testing.compile.JavaSourceSubjectFactory;

import org.junit.Test;

import javax.tools.JavaFileObject;

public class ApiGeneratorProcessorTest {

  ApiGeneratorProcessor processor = new ApiGeneratorProcessor();

  @Test
  public void shouldCompilePlainTaskBinding() {
    JavaFileObject source = JavaFileObjects.forResource("source/GeneratedApiTemplate.java");
    Truth.assert_().about(JavaSourceSubjectFactory.javaSource())
        .that(source)
        .processedWith(processor)
        .compilesWithoutError()
        .and()
        .generatesSources(JavaFileObjects.forResource("generated/GeneratedApi.java"));
  }
}
