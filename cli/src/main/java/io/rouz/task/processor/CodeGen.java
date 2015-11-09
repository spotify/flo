package io.rouz.task.processor;

import io.rouz.task.cli.TaskConstructor;
import com.squareup.javapoet.AnnotationSpec;
import com.squareup.javapoet.JavaFile;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.ParameterSpec;
import com.squareup.javapoet.TypeName;
import com.squareup.javapoet.TypeSpec;

import java.util.List;
import java.util.stream.Collectors;

import javax.annotation.Generated;
import javax.lang.model.element.Modifier;
import javax.lang.model.element.Name;
import javax.lang.model.type.DeclaredType;
import javax.lang.model.type.TypeMirror;

import joptsimple.OptionParser;
import joptsimple.OptionSpecBuilder;

import static com.squareup.javapoet.MethodSpec.constructorBuilder;
import static com.squareup.javapoet.MethodSpec.methodBuilder;
import static com.squareup.javapoet.TypeSpec.classBuilder;
import static javax.lang.model.element.Modifier.FINAL;
import static javax.lang.model.element.Modifier.PRIVATE;
import static javax.lang.model.element.Modifier.PUBLIC;
import static javax.lang.model.element.Modifier.STATIC;
import static javax.lang.model.type.TypeKind.DECLARED;
import static javax.tools.Diagnostic.Kind.ERROR;
import static javax.tools.Diagnostic.Kind.NOTE;

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
        .addModifiers(PUBLIC, FINAL);

    bindings.stream()
        .map(Binding::method)
        .map(util::enclosingClass)
        .forEachOrdered(factoryClassBuilder::addOriginatingElement);

    factoryClassBuilder.addMethod(
        constructorBuilder()
            .addModifiers(PRIVATE)
            .addStatement("// no instantiation")
            .build());

    bindings.stream()
        .map(this::binderMethod)
        .forEachOrdered(factoryClassBuilder::addMethod);

    bindings.stream()
        .map(this::taskConstructor)
        .forEachOrdered(factoryClassBuilder::addType);

    factoryClassBuilder.addMethod(optHelper());

    return JavaFile.builder(commonPackage.toString(), factoryClassBuilder.build())
        .skipJavaLangImports(true)
        .build();
  }

  private TypeSpec taskConstructor(Binding binding) {
    util.messager.printMessage(NOTE, "Binding " + binding);

    final Name enclosingName = binding.enclosingClass().getSimpleName();
    final String taskName = capitalize(binding.name());
    final DeclaredType taskType = (DeclaredType) binding.returnType();
    final TypeMirror taskArg = taskType.getTypeArguments().get(0);

    return classBuilder(enclosingName + "_" + taskName)
        .addModifiers(PRIVATE, STATIC, FINAL)
        .addSuperinterface(TypeName.get(util.typeWithArgs(TaskConstructor.class, taskArg)))

        // public String name() {
        .addMethod(
            methodBuilder("name")
                .addAnnotation(Override.class)
                .addModifiers(PUBLIC)
                .returns(String.class)
                .addStatement("return $S", enclosingName + "." + binding.name())
                .build())

        // public Task<T> create(String... args) {
        .addMethod(
            methodBuilder("create")
                .addAnnotation(Override.class)
                .addModifiers(PUBLIC)
                .returns(TypeName.get(taskType))
                .addParameter(String[].class, "args")
                .varargs()
                .addStatement("return $N.$N()", enclosingName, binding.name()) // TODO: args
                .build())

        // public OptionParser parser() {
        .addMethod(
            methodBuilder("parser")
                .addAnnotation(Override.class)
                .addModifiers(PUBLIC)
                .returns(OptionParser.class)
                .addStatement("final $T parser = new $T()", OptionParser.class, OptionParser.class)
                .addStatement("return parser")
                .build())

        .build();
  }

  private MethodSpec binderMethod(Binding binding) {
    final MethodSpec.Builder methodBuilder = methodBuilder(binding.name().toString())
        .addModifiers(PUBLIC, STATIC)
        .returns(TypeName.get(binding.returnType()))
        .addParameter(TypeName.get(util.mapStringString()), ARGS);

    for (Binding.Argument argument : binding.arguments()) {
      final TypeMirror type = argument.type().getKind() == DECLARED
                              ? util.refresh(argument.type())
                              : argument.type();

      if (util.types.isAssignable(util.typeMirror(String.class), type)) {
        methodBuilder.addStatement(
            "final $T $N = $N.get($S)",
            String.class, argument.name(),
            ARGS, argument.name());
      } else
      if (util.types.isAssignable(util.typeMirror(Double.class), type)) {
        methodBuilder.addStatement(
            "final $T $N = $T.parseDouble($N.get($S))",
            double.class, argument.name(),
            Double.class,
            ARGS, argument.name());
      } else
      if (util.types.isAssignable(util.typeMirror(Integer.class), type)) {
        methodBuilder.addStatement(
            "final $T $N = $T.parseInt($N.get($S))",
            int.class, argument.name(),
            Integer.class,
            ARGS, argument.name());
      } else {
        util.messager.printMessage(
            ERROR,
            "Unsupported argument type for " + TaskBindingProcessor.ROOT + " annotation: " + type,
            binding.method());
        throw new RuntimeException("abort");
      }
    }

    final String callArgs = binding.arguments().stream()
        .map(Binding.Argument::name)
        .collect(Collectors.joining(", "));

    return methodBuilder
        .addStatement(
            "return $T.$N(" + callArgs + ")",
            binding.enclosingClass(),
            binding.name())
        .build();
  }

  private MethodSpec optHelper() {
    return methodBuilder("opt")
        .addModifiers(PRIVATE, STATIC)
        .addParameter(TypeName.get(String.class), "name")
        .addParameter(TypeName.get(Class.class), "type")
        .addParameter(TypeName.get(OptionParser.class), "parser")

        .addStatement(
            "final $T isFlag = $T.class.equals(type)",
            boolean.class, boolean.class)

        .addStatement(
            "final $T spec = (isFlag)\n ? parser.accepts(name, $S)\n : parser.accepts(name)",
            OptionSpecBuilder.class, "(default: false)")

        .addCode("\n")

        .beginControlFlow("if (!isFlag)")
        .addStatement("spec.withRequiredArg().ofType(type).describedAs(name).required()")
        .endControlFlow()

        .build();
  }

  private static String capitalize(Name name) {
    return Character.toUpperCase(name.charAt(0)) + name.toString().substring(1);
  }
}
