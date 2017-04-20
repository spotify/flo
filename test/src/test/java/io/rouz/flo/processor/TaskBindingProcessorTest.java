package io.rouz.flo.processor;

import static com.google.common.truth.Truth.assert_;
import static com.google.testing.compile.JavaFileObjects.forResource;
import static com.google.testing.compile.JavaSourceSubjectFactory.javaSource;

import javax.tools.JavaFileObject;
import org.junit.Test;

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

  // test help
//  if (parse.has("h")) {
//    parser.printHelpOn(System.err);
//    System.exit(1); // maybe not exit
//  }

//  @Test
//  public void shouldPutFactoryInCommonPackage() throws Exception {
//    JavaFileObject source1 = forResource("compiling/Sibling1.java");
//    JavaFileObject source2 = forResource("compiling/Sibling2.java");
//    assert_().about(javaSources())
//        .that(Arrays.asList(source1, source2))
//        .processedWith(processor)
//        .compilesWithoutError()
//        .and()
//        .generatesSources(
//            forResource("out/Sibling1Factory.java"),
//            forResource("out/Sibling2Factory.java"));
//  }

  @Test
  public void failOnMethodNotReturningTask() {
    JavaFileObject source = forResource("failing/MethodNotReturningTask.java");
    assert_().about(javaSource())
        .that(source)
        .processedWith(processor)
        .failsToCompile()
        .withErrorContaining("annotated method must return a io.rouz.flo.Task<?>");
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
