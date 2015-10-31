package io.rouz.task.processor;

import io.rouz.task.Task;
import com.squareup.javapoet.AnnotationSpec;
import com.squareup.javapoet.JavaFile;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.TypeName;
import com.squareup.javapoet.TypeSpec;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;

import javax.annotation.Generated;
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
import javax.lang.model.type.DeclaredType;
import javax.lang.model.type.TypeMirror;
import javax.lang.model.util.Elements;
import javax.lang.model.util.Types;

import static com.squareup.javapoet.MethodSpec.constructorBuilder;
import static com.squareup.javapoet.MethodSpec.methodBuilder;
import static com.squareup.javapoet.TypeSpec.classBuilder;
import static java.util.Optional.empty;
import static java.util.Optional.of;
import static javax.lang.model.type.TypeKind.INT;
import static javax.tools.Diagnostic.Kind.ERROR;
import static javax.tools.Diagnostic.Kind.NOTE;

/**
 * TODO: document.
 */
@SupportedSourceVersion(SourceVersion.RELEASE_8)
public class TaskBindingProcessor extends AbstractProcessor {

  private static final AnnotationSpec GENERATED_ANNOTATION = AnnotationSpec.builder(Generated.class)
      .addMember("value", "$S", TaskBindingProcessor.class.getCanonicalName())
      .build();

  private static final String ROOT = "@" + RootTask.class.getSimpleName();
  private static final String ARGS = "$args";

  private Types typeUtils;
  private Elements elementUtils;
  private Filer filer;
  private Messager messager;

  private List<Binding> bindings = new ArrayList<>();

  @Override
  public synchronized void init(ProcessingEnvironment processingEnv) {
    super.init(processingEnv);
    typeUtils = processingEnv.getTypeUtils();
    elementUtils = processingEnv.getElementUtils();
    filer = processingEnv.getFiler();
    messager = processingEnv.getMessager();

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
    boolean newBindings = false;

    for (TypeElement annotation : annotations) {
      messager.printMessage(NOTE, "Processing @" + annotation.getSimpleName() + " annotation");
      for (Element element : roundEnv.getElementsAnnotatedWith(annotation)) {
        if (element.getKind() != ElementKind.METHOD) {
          messager.printMessage(ERROR, "only methods can be annotated", element);
          return true;
        }

        Optional<Binding> binding = createBinding((ExecutableElement) element);
        binding.ifPresent(bindings::add);
        newBindings |= binding.isPresent();
      }
    }

    if (!newBindings && !bindings.isEmpty()) {
      for (Binding binding : bindings) {
        messager.printMessage(NOTE, "Binding " + binding);
      }

      try {
        bindingFactory(bindings).writeTo(filer);
      } catch (IOException e) {
        messager.printMessage(ERROR, "Failed to write source for " + ROOT + " bindings: " + e);
      }
      bindings.clear();
    }

    return newBindings;
  }

  private Optional<Binding> createBinding(ExecutableElement method) {
    if (!validate(method)) {
      return empty();
    }

    final List<Binding.Argument> args = new ArrayList<>();

    messager.printMessage(NOTE, "parameters:");
    for (VariableElement variableElement : method.getParameters()) {
      args.add(Binding.argument(variableElement.getSimpleName(), variableElement.asType()));
      messager.printMessage(NOTE, "  name: " + variableElement.getSimpleName());
      messager.printMessage(NOTE, "  type: " + variableElement.asType().toString());
      messager.printMessage(NOTE, "  ---");
    }
    messager.printMessage(NOTE, "---");

    final TypeElement enclosingClass = enclosingClass(method);
    final TypeMirror returnType = method.getReturnType();
    final Name name = method.getSimpleName();

    return of(Binding.create(method, enclosingClass, returnType, name, args));
  }

  private JavaFile bindingFactory(List<Binding> bindings) {
    final Name commonPackage = commonPackage(bindings).getQualifiedName();

    final TypeSpec.Builder factoryClassBuilder = classBuilder("NameMeFactory")
        .addAnnotation(GENERATED_ANNOTATION)
        .addModifiers(Modifier.PUBLIC, Modifier.FINAL);

    factoryClassBuilder.addMethod(
        constructorBuilder()
            .addModifiers(Modifier.PRIVATE)
            .addStatement("// no instantiation")
            .build());

    bindings.stream()
        .map(this::binderMethod)
        .forEachOrdered(factoryClassBuilder::addMethod);

    /*
    public static List<Function<Map<String, String>, Task<?>>> constructors() {
      final List<Function<Map<String, String>, Task<?>>> constructors = new ArrayList<>();
      constructors.add(NameMeFactory::foo);
      constructors.add(NameMeFactory::bar);
      constructors.add(NameMeFactory::higherUp);
      return Collections.unmodifiableList(constructors);
    }
     */

    return JavaFile.builder(commonPackage.toString(), factoryClassBuilder.build())
        .build();
  }

  private MethodSpec binderMethod(Binding binding) {
    final MethodSpec.Builder methodBuilder = methodBuilder(binding.name().toString())
        .addModifiers(Modifier.PUBLIC, Modifier.STATIC)
        .returns(TypeName.get(binding.returnType()))
        .addParameter(TypeName.get(mapStringString()), ARGS);

    for (Binding.Argument argument : binding.arguments()) {
      // FIXME: this doesn't work
      if (typeUtils.isAssignable(typeMirror(String.class), argument.type())) {
        methodBuilder.addStatement(
            "$T $N = $N.get($S)",
            String.class, argument.name(),
            ARGS, argument.name());
      }
      if (typeUtils.isAssignable(typeUtils.getPrimitiveType(INT), argument.type())) {
        methodBuilder.addStatement(
            "$T $N = $T.parseInt($N.get($S))",
            int.class, argument.name(),
            Integer.class,
            ARGS, argument.name());
      }
    }

    return methodBuilder
        .addStatement("return $T.$N()", binding.enclosingClass(), binding.name())
        .build();
  }

  private boolean validate(ExecutableElement method) {
    final TypeMirror returnType = method.getReturnType();
    final Set<Modifier> methodModifiers = method.getModifiers();
    final Set<Modifier> classModifiers = enclosingClass(method).getModifiers();

    if (!typeUtils.isAssignable(returnType, taskWildcard())) {
      messager.printMessage(ERROR, ROOT + " annotated method must return a " + taskWildcard(), method);
      return false;
    }

    if (!methodModifiers.contains(Modifier.STATIC)) {
      messager.printMessage(ERROR, ROOT + " annotated method must be static", method);
      return false;
    }

    // TODO: only if factory is in same package as all bindings
    if (methodModifiers.contains(Modifier.PRIVATE)) {
      messager.printMessage(ERROR, ROOT + " annotated method must not be private", method);
      return false;
    }

    if (methodModifiers.contains(Modifier.PROTECTED)) {
      messager.printMessage(ERROR, ROOT + " annotated method must not be protected", method);
      return false;
    }

    // TODO: only if factory is placed in different package
//    if (!methodModifiers.contains(Modifier.PUBLIC)) {
//      messager.printMessage(ERROR, ROOT + " annotated method must be public", method);
//      return false;
//    }
//
//    if (!classModifiers.contains(Modifier.PUBLIC)) {
//      messager.printMessage(ERROR, ROOT + " annotated method must be in public class", method);
//      return false;
//    }

    return true;
  }

  private PackageElement commonPackage(List<Binding> bindings) {
    class RecursiveMap extends LinkedHashMap<String, RecursiveMap> {}
    final RecursiveMap packages = new RecursiveMap();

    for (Binding binding : bindings) {
      final PackageElement packageElement = packageOf(binding.method());
      final String[] parts = packageElement.getQualifiedName().toString().split("\\.");

      RecursiveMap node = packages;
      for (String part : parts) {
        node = node.computeIfAbsent(part, p -> new RecursiveMap());
      }
    }

    messager.printMessage(NOTE, "package tree: " + packages);

    String common = "";
    RecursiveMap node = packages;
    while (node.size() == 1) {
      final Entry<String, RecursiveMap> next = node.entrySet().iterator().next();
      common += (common.isEmpty() ? "" : ".") + next.getKey();
      node = next.getValue();
    }
    return elementUtils.getPackageElement(common);
  }

  private <T> TypeMirror typeMirror(Class<? extends T> clazz) {
    return typeElement(clazz).asType();
  }

  private DeclaredType taskWildcard() {
    final TypeElement task = typeElement(Task.class);
    return typeUtils.getDeclaredType(task, typeUtils.getWildcardType(null, null));
  }

  private DeclaredType mapStringString() {
    final TypeElement map = typeElement(Map.class);
    final TypeElement string = typeElement(String.class);
    return typeUtils.getDeclaredType(map, string.asType(), string.asType());
  }

  private <T> TypeElement typeElement(Class<? extends T> clazz) {
    return elementUtils.getTypeElement(clazz.getCanonicalName());
  }

  private PackageElement packageOf(Element element) {
    return elementUtils.getPackageOf(element);
  }

  private static TypeElement enclosingClass(Element element) {
    if (element.getKind() != ElementKind.CLASS) {
      return enclosingClass(element.getEnclosingElement());
    }

    return (TypeElement) element;
  }
}
