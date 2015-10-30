package io.rouz.task.processor;

import com.google.auto.service.AutoService;

import io.rouz.task.Task;

import java.util.LinkedHashSet;
import java.util.Set;

import javax.annotation.processing.AbstractProcessor;
import javax.annotation.processing.Filer;
import javax.annotation.processing.Messager;
import javax.annotation.processing.ProcessingEnvironment;
import javax.annotation.processing.Processor;
import javax.annotation.processing.RoundEnvironment;
import javax.annotation.processing.SupportedSourceVersion;
import javax.lang.model.SourceVersion;
import javax.lang.model.element.Element;
import javax.lang.model.element.ElementKind;
import javax.lang.model.element.ExecutableElement;
import javax.lang.model.element.Modifier;
import javax.lang.model.element.TypeElement;
import javax.lang.model.element.VariableElement;
import javax.lang.model.type.DeclaredType;
import javax.lang.model.type.TypeMirror;
import javax.lang.model.util.Elements;
import javax.lang.model.util.Types;

import static javax.tools.Diagnostic.Kind.ERROR;
import static javax.tools.Diagnostic.Kind.NOTE;

/**
 * TODO: document.
 */
@SupportedSourceVersion(SourceVersion.RELEASE_8)
@AutoService(Processor.class)
public class TaskBindingProcessor extends AbstractProcessor {

  private final String ROOT = RootTask.class.getSimpleName();

  private Types typeUtils;
  private Elements elementUtils;
  private Filer filer;
  private Messager messager;

  DeclaredType taskWildcard;

  @Override
  public synchronized void init(ProcessingEnvironment processingEnv) {
    super.init(processingEnv);
    typeUtils = processingEnv.getTypeUtils();
    elementUtils = processingEnv.getElementUtils();
    filer = processingEnv.getFiler();
    messager = processingEnv.getMessager();

    taskWildcard = getWildcardTaskType();

    messager.printMessage(NOTE, taskWildcard.toString());
    messager.printMessage(NOTE, TaskBindingProcessor.class.getSimpleName() + " loaded");
  }

  @Override
  public Set<String> getSupportedAnnotationTypes() {
    Set<String> annotationTypes = new LinkedHashSet<>();
    annotationTypes.add(RootTask.class.getCanonicalName());
    return annotationTypes;
  }

  @Override
  public boolean process(Set<? extends TypeElement> annotations, RoundEnvironment roundEnv) {
    for (TypeElement annotation : annotations) {
      messager.printMessage(NOTE, "Processing " + annotation.getSimpleName());
      for (Element element : roundEnv.getElementsAnnotatedWith(annotation)) {
        if (element.getKind() != ElementKind.METHOD) {
          messager.printMessage(ERROR, "only methods can be annotated", element);
          return true;
        }
        messager.printMessage(NOTE, "found annotated method", element);

        createRoot((ExecutableElement) element);
      }
    }

    return true;
  }

  private void createRoot(ExecutableElement method) {
    if (!validate(method)) {
      return;
    }

    messager.printMessage(NOTE, "name " + method.getSimpleName());
    messager.printMessage(NOTE, "return type " + method.getReturnType());
    messager.printMessage(NOTE, "of Task<?> " + typeUtils.isAssignable(method.getReturnType(), taskWildcard));
    messager.printMessage(NOTE, "parameters:");
    for (VariableElement variableElement : method.getParameters()) {
      messager.printMessage(NOTE, "  name: " + variableElement.getSimpleName());
      messager.printMessage(NOTE, "  type: " + variableElement.asType().toString());
      messager.printMessage(NOTE, "  ---");
    }
  }

  private boolean validate(ExecutableElement method) {
    final Set<Modifier> modifiers = method.getModifiers();
    if (!modifiers.contains(Modifier.STATIC)) {
      messager.printMessage(
          ERROR,
          ROOT + " annotated method must be static",
          method);

      return false;
    }

    final TypeMirror returnType = method.getReturnType();
    if (!typeUtils.isAssignable(returnType, taskWildcard)) {
      messager.printMessage(
          ERROR,
          ROOT + " annotated method must return a " + taskWildcard,
          method);

      return false;
    }

    return true;
  }

  private DeclaredType getWildcardTaskType() {
    TypeElement taskElement = elementUtils.getTypeElement(Task.class.getCanonicalName());
    return typeUtils.getDeclaredType(taskElement, typeUtils.getWildcardType(null, null));
  }
}
