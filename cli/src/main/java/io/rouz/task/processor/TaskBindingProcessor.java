package io.rouz.task.processor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import javax.annotation.processing.AbstractProcessor;
import javax.annotation.processing.Filer;
import javax.annotation.processing.Messager;
import javax.annotation.processing.ProcessingEnvironment;
import javax.annotation.processing.RoundEnvironment;
import javax.annotation.processing.SupportedSourceVersion;
import javax.lang.model.SourceVersion;
import javax.lang.model.element.Element;
import javax.lang.model.element.ElementKind;
import javax.lang.model.element.ExecutableElement;
import javax.lang.model.element.Modifier;
import javax.lang.model.element.Name;
import javax.lang.model.element.PackageElement;
import javax.lang.model.element.TypeElement;
import javax.lang.model.element.VariableElement;
import javax.lang.model.type.TypeMirror;
import javax.lang.model.util.Elements;
import javax.lang.model.util.Types;

import static java.util.Optional.empty;
import static java.util.Optional.of;
import static javax.tools.Diagnostic.Kind.ERROR;
import static javax.tools.Diagnostic.Kind.NOTE;

/**
 * TODO: document.
 */
@SupportedSourceVersion(SourceVersion.RELEASE_8)
public class TaskBindingProcessor extends AbstractProcessor {

  static final String ROOT = "@" + RootTask.class.getSimpleName();

  private Types types;
  private Elements element;
  private Filer filer;
  private Messager messager;

  private ProcessorUtil util;
  private CodeGen codeGen;

  private final Map<String, List<Binding>> bindingsByPackage = new HashMap<>();

  @Override
  public synchronized void init(ProcessingEnvironment processingEnv) {
    super.init(processingEnv);
    types = processingEnv.getTypeUtils();
    element = processingEnv.getElementUtils();
    filer = processingEnv.getFiler();
    messager = processingEnv.getMessager();

    util = new ProcessorUtil(types, element, messager);
    codeGen = new CodeGen(util);

    messager.printMessage(NOTE, TaskBindingProcessor.class.getSimpleName() + " loaded");
  }

  @Override
  public Set<String> getSupportedAnnotationTypes() {
    final Set<String> annotationTypes = new LinkedHashSet<>();
    annotationTypes.add(RootTask.class.getCanonicalName());
    return annotationTypes;
  }

  @Override
  public boolean process(Set<? extends TypeElement> annotations, RoundEnvironment roundEnv) {
    for (TypeElement annotation : annotations) {
      messager.printMessage(NOTE, "Processing @" + annotation.getSimpleName() + " annotation");
      for (Element element : roundEnv.getElementsAnnotatedWith(annotation)) {
        if (element.getKind() != ElementKind.METHOD) {
          messager.printMessage(ERROR, "only methods can be annotated with " + ROOT, element);
          return true;
        }

        Optional<Binding> binding = createBinding((ExecutableElement) element);
        binding.ifPresent(this::collectBinding);
      }
    }

    if (!bindingsByPackage.isEmpty()) {
      for (List<Binding> binding : bindingsByPackage.values()) {
        messager.printMessage(NOTE, "Binding " + binding);
        try {
          codeGen.bindingFactory(binding).writeTo(filer);
        } catch (IOException e) {
          messager.printMessage(ERROR, "Failed to write source for " + ROOT + " bindings: " + e);
        } catch (RuntimeException e) {
          messager.printMessage(ERROR, "Error during " + ROOT + " binding generation");
        }
      }
      bindingsByPackage.clear();
    }

    return true;
  }

  private void collectBinding(Binding binding) {
    final PackageElement bindingPackage = element.getPackageOf(binding.method());
    bindingsByPackage
        .computeIfAbsent(bindingPackage.toString(), p -> new ArrayList<>())
        .add(binding);
  }

  private Optional<Binding> createBinding(ExecutableElement method) {
    if (!validate(method)) {
      return empty();
    }

    final List<Binding.Argument> args = new ArrayList<>();

    messager.printMessage(NOTE, "parameters:");
    for (VariableElement variableElement : method.getParameters()) {
      final Name name = variableElement.getSimpleName();
      final TypeMirror type = variableElement.asType();

      args.add(Binding.argument(name, type));
      messager.printMessage(NOTE, "  name: " + name);
      messager.printMessage(NOTE, "  type: " + type.toString());
      messager.printMessage(NOTE, "  ---");
    }

    messager.printMessage(NOTE, "---");

    final TypeElement enclosingClass = util.enclosingClass(method);
    final TypeMirror returnType = method.getReturnType();
    final Name name = method.getSimpleName();

    return of(Binding.create(method, enclosingClass, returnType, name, args));
  }

  private boolean validate(ExecutableElement method) {
    final TypeMirror returnType = method.getReturnType();
    final Set<Modifier> methodModifiers = method.getModifiers();

    if (!types.isAssignable(returnType, util.taskWildcard())) {
      messager.printMessage(ERROR, ROOT + " annotated method must return a " + util.taskWildcard(), method);
      return false;
    }

    if (!methodModifiers.contains(Modifier.STATIC)) {
      messager.printMessage(ERROR, ROOT + " annotated method must be static", method);
      return false;
    }

    if (methodModifiers.contains(Modifier.PRIVATE)) {
      messager.printMessage(ERROR, ROOT + " annotated method must not be private", method);
      return false;
    }

    if (methodModifiers.contains(Modifier.PROTECTED)) {
      messager.printMessage(ERROR, ROOT + " annotated method must not be protected", method);
      return false;
    }

    return true;
  }
}
