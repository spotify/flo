package io.rouz.task.processor;

import com.squareup.javapoet.AnnotationSpec;
import com.squareup.javapoet.JavaFile;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.TypeName;
import com.squareup.javapoet.TypeSpec;

import java.util.List;

import javax.annotation.Generated;
import javax.lang.model.element.Modifier;
import javax.lang.model.element.Name;
import javax.lang.model.type.TypeMirror;

import static io.rouz.task.processor.TaskBindingProcessor.ROOT;
import static com.squareup.javapoet.MethodSpec.constructorBuilder;
import static com.squareup.javapoet.MethodSpec.methodBuilder;
import static com.squareup.javapoet.TypeSpec.classBuilder;
import static javax.lang.model.type.TypeKind.DECLARED;
import static javax.tools.Diagnostic.Kind.ERROR;

/**
 * Class for generating {@link JavaFile} sources from {@link Binding} instances
 */
final class CodeGen {

  private static final String ARGS = "$args";
  private static final AnnotationSpec GENERATED_ANNOTATION = AnnotationSpec.builder(Generated.class)
      .addMember("value", "$S", TaskBindingProcessor.class.getCanonicalName())
      .build();

  private final ProcessorUtil util;

  CodeGen(ProcessorUtil util) {
    this.util = util;
  }

  JavaFile bindingFactory(List<Binding> bindings) {
    final Name commonPackage = util.commonPackage(bindings).getQualifiedName();

    final TypeSpec.Builder factoryClassBuilder = classBuilder("FloRootTaskFactory")
        .addAnnotation(GENERATED_ANNOTATION)
        .addModifiers(Modifier.PUBLIC, Modifier.FINAL);

    bindings.stream()
        .map(Binding::method)
        .map(util::enclosingClass)
        .forEachOrdered(factoryClassBuilder::addOriginatingElement);

    factoryClassBuilder.addMethod(
        constructorBuilder()
            .addModifiers(Modifier.PRIVATE)
            .addStatement("// no instantiation")
            .build());

    bindings.stream()
        .map(this::binderMethod)
        .forEachOrdered(factoryClassBuilder::addMethod);

    return JavaFile.builder(commonPackage.toString(), factoryClassBuilder.build())
        .skipJavaLangImports(true)
        .build();
  }

  private MethodSpec binderMethod(Binding binding) {
    final MethodSpec.Builder methodBuilder = methodBuilder(binding.name().toString())
        .addModifiers(Modifier.PUBLIC, Modifier.STATIC)
        .returns(TypeName.get(binding.returnType()))
        .addParameter(TypeName.get(util.mapStringString()), ARGS);

    final StringBuilder sb = new StringBuilder();
    for (Binding.Argument argument : binding.arguments()) {
      final TypeMirror type = argument.type().getKind() == DECLARED
                              ? util.refresh(argument.type())
                              : argument.type();

      if (util.types.isAssignable(util.typeMirror(String.class), type)) {
        methodBuilder.addStatement(
            "$T $N = $N.get($S)",
            String.class, argument.name(),
            ARGS, argument.name());
      } else
      if (util.types.isAssignable(util.typeMirror(Double.class), type)) {
        methodBuilder.addStatement(
            "$T $N = $T.parseDouble($N.get($S))",
            double.class, argument.name(),
            Double.class,
            ARGS, argument.name());
      } else
      if (util.types.isAssignable(util.typeMirror(Integer.class), type)) {
        methodBuilder.addStatement(
            "$T $N = $T.parseInt($N.get($S))",
            int.class, argument.name(),
            Integer.class,
            ARGS, argument.name());
      } else {
        util.messager.printMessage(
            ERROR,
            "Unsupported argument type for " + ROOT + " annotation: " + type,
            binding.method());
        throw new RuntimeException("abort");
      }

      sb.append(argument.name()).append(", ");
    }

    final String callArgs = sb.substring(0, Math.max(sb.length() - 2, 0));
    return methodBuilder
        .addStatement(
            "return $T.$N(" + callArgs + ")",
            binding.enclosingClass(),
            binding.name())
        .build();
  }
}
