package io.rouz.flo.gen;

import org.trimou.engine.MustacheEngine;
import org.trimou.engine.MustacheEngineBuilder;
import org.trimou.engine.locator.ClassPathTemplateLocator;
import org.trimou.lambda.Lambda;
import org.trimou.lambda.SimpleLambdas;

import java.io.IOException;
import java.io.Writer;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import javax.annotation.processing.AbstractProcessor;
import javax.annotation.processing.Filer;
import javax.annotation.processing.Messager;
import javax.annotation.processing.ProcessingEnvironment;
import javax.annotation.processing.RoundEnvironment;
import javax.annotation.processing.SupportedSourceVersion;
import javax.lang.model.SourceVersion;
import javax.lang.model.element.Element;
import javax.lang.model.element.ElementKind;
import javax.lang.model.element.Name;
import javax.lang.model.element.TypeElement;
import javax.lang.model.util.Elements;
import javax.lang.model.util.Types;
import javax.tools.JavaFileObject;

import static javax.tools.Diagnostic.Kind.ERROR;
import static javax.tools.Diagnostic.Kind.NOTE;

/**
 * TODO: document.
 */
@SupportedSourceVersion(SourceVersion.RELEASE_8)
public class ApiGeneratorProcessor extends AbstractProcessor {

  static final String ANNOTATION = "@" + GenerateTaskBuilder.class.getSimpleName();

  private Types types;
  private Elements elements;
  private Filer filer;
  private Messager messager;

  private MustacheEngine engine;

  @Override
  public synchronized void init(ProcessingEnvironment processingEnv) {
    super.init(processingEnv);
    types = processingEnv.getTypeUtils();
    elements = processingEnv.getElementUtils();
    filer = processingEnv.getFiler();
    messager = processingEnv.getMessager();

    engine = MustacheEngineBuilder
        .newBuilder()
        .addTemplateLocator(ClassPathTemplateLocator.builder(1).setSuffix("mustache").build())
        .addGlobalData("typeArgs", lambda(this::typeArgs))
        .addGlobalData("parameters", lambda(this::parameters))
        .addGlobalData("jdkInterface", lambda(this::jdkInterface))
        .build();

    messager.printMessage(NOTE, ApiGeneratorProcessor.class.getSimpleName() + " loaded");
  }

  @Override
  public Set<String> getSupportedAnnotationTypes() {
    final Set<String> annotationTypes = new LinkedHashSet<>();
    annotationTypes.add(GenerateTaskBuilder.class.getCanonicalName());
    return annotationTypes;
  }

  @Override
  public boolean process(Set<? extends TypeElement> annotations, RoundEnvironment roundEnv) {
    for (TypeElement annotation : annotations) {
      for (Element element : roundEnv.getElementsAnnotatedWith(annotation)) {
        if (element.getKind() != ElementKind.INTERFACE) {
          messager.printMessage(ERROR, "only interfaces can be annotated with " + ANNOTATION, element);
          return true;
        }

        final GenerateTaskBuilder genTaskBuilder = element.getAnnotation(GenerateTaskBuilder.class);
        final TypeElement templateElement = (TypeElement) element;

        try {
          final Name packageName = elements.getPackageOf(templateElement).getQualifiedName();
          final String className = templateElement.getSimpleName().toString().replaceAll("Template$", "");

          final Map<String, Object> data = new HashMap<>();
          data.put("genUpTo", new Object[genTaskBuilder.upTo() + 1]);
          data.put("packageName", packageName);
          data.put("className", className);
          final String output = engine.getMustache("TaskBuilder").render(data);

          final String fileName = packageName + "." + className;
          final JavaFileObject filerSourceFile = filer.createSourceFile(fileName, element);
          try (final Writer writer = filerSourceFile.openWriter()) {
            writer.write(output);
          }
        } catch (IOException e) {
          messager.printMessage(ERROR, "Failed to write source for " + ANNOTATION + " bindings: " + e);
        } catch (RuntimeException e) {
          e.printStackTrace();
          messager.printMessage(ERROR, "Error during " + ANNOTATION + " binding generation");
        }
      }
    }

    return true;
  }

  private Object lambda(Function<String, String> fn) {
    return SimpleLambdas.builder()
        .inputType(Lambda.InputType.PROCESSED)
        .invoke(fn)
        .build();
  }

  private Stream<String> letters(int n) {
    return IntStream.range(0, n)
        .mapToObj(i -> Character.toString((char) ('A' + i)));
  }

  private String typeArgs(String nStr) {
    final int n = Integer.parseInt(nStr);
    return letters(n)
               .collect(Collectors.joining(", "))
           + (n > 0 ? ", " : "");
  }

  private String parameters(String nStr) {
    final int n = Integer.parseInt(nStr);
    return letters(n)
        .map(l -> l + " " + l.toLowerCase())
        .collect(Collectors.joining(", "));
  }

  private String jdkInterface(String nStr) {
    final int n = Integer.parseInt(nStr);
    switch (n) {
      case 0: return "Supplier<Z>, ";
      case 1: return "Function<A, Z>, ";
      case 2: return "BiFunction<A, B, Z>, ";
      default: return "";
    }
  }
}
