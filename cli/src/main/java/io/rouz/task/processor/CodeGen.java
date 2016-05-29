package io.rouz.task.processor;

import com.squareup.javapoet.AnnotationSpec;
import com.squareup.javapoet.JavaFile;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.TypeName;
import com.squareup.javapoet.TypeSpec;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import javax.annotation.Generated;
import javax.lang.model.element.Name;
import javax.lang.model.type.DeclaredType;
import javax.lang.model.type.TypeKind;
import javax.lang.model.type.TypeMirror;

import io.rouz.task.cli.TaskConstructor;
import io.rouz.task.processor.Binding.Argument;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpecBuilder;

import static com.squareup.javapoet.MethodSpec.constructorBuilder;
import static com.squareup.javapoet.MethodSpec.methodBuilder;
import static com.squareup.javapoet.TypeSpec.classBuilder;
import static java.util.stream.Collectors.toList;
import static javax.lang.model.element.Modifier.FINAL;
import static javax.lang.model.element.Modifier.PRIVATE;
import static javax.lang.model.element.Modifier.PUBLIC;
import static javax.lang.model.element.Modifier.STATIC;
import static javax.tools.Diagnostic.Kind.NOTE;

/**
 * Class for generating {@link JavaFile} sources from {@link Binding} instances
 */
final class CodeGen {

  private static final AnnotationSpec GENERATED_ANNOTATION = AnnotationSpec.builder(Generated.class)
      .addMember("value", "$S", TaskBindingProcessor.class.getCanonicalName())
      .build();

  private final ProcessorUtil util;

  CodeGen(ProcessorUtil util) {
    this.util = util;
  }

  JavaFile bindingFactory(List<Binding> bindings) {
    final Name commonPackage = util.commonPackage(bindings).getQualifiedName();
    final List<TypeSpec> taskConstructors = bindings.stream()
        .map(this::taskConstructor)
        .collect(toList());

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

    taskConstructors.stream()
        .map(this::taskConstructorStaticConstructor)
        .forEachOrdered(factoryClassBuilder::addMethod);

    factoryClassBuilder.addMethod(optHelper());

    taskConstructors.stream()
        .forEachOrdered(factoryClassBuilder::addType);

    return JavaFile.builder(commonPackage.toString(), factoryClassBuilder.build())
        .skipJavaLangImports(true)
        .build();
  }

  private MethodSpec taskConstructorStaticConstructor(TypeSpec taskConstructor) {
    return methodBuilder(taskConstructor.name)
        .addModifiers(PUBLIC, STATIC)
        .returns(taskConstructor.superinterfaces.get(0))
        .addStatement("return new $N()", taskConstructor)
        .build();
  }

  private TypeSpec taskConstructor(Binding binding) {
    util.messager.printMessage(NOTE, "Binding " + binding);

    final Name enclosingName = binding.enclosingClass().getSimpleName();
    final String taskName = capitalize(binding.name());
    final DeclaredType taskType = (DeclaredType) binding.returnType();
    final TypeMirror taskArg = taskType.getTypeArguments().get(0);
    final List<Argument> arguments = binding.arguments();

    // public String name() {
    final MethodSpec name = methodBuilder("name")
        .addAnnotation(Override.class)
        .addModifiers(PUBLIC)
        .returns(String.class)
        .addStatement("return $S", enclosingName + "." + binding.name())
        .build();

    // public Task<T> create(String... args) {
    final MethodSpec.Builder createBuilder = methodBuilder("create")
        .addAnnotation(Override.class)
        .addModifiers(PUBLIC)
        .returns(TypeName.get(taskType))
        .addParameter(String[].class, "args")
        .varargs()
        .addStatement("final $T parser = parser()", OptionParser.class)
        .addStatement("final $T parse = parser.parse(args)", OptionSet.class)
        .addCode("\n");

    for (Argument argument : arguments) {
      if (util.types.isSameType(util.types.getPrimitiveType(TypeKind.BOOLEAN), argument.type())) {
        createBuilder.addStatement(
            "final $T $N = parse.has($S)",
            argument.type(), argument.name(), argument.name());
      } else {
        createBuilder.addStatement(
            "final $T $N = ($T) $T.requireNonNull(parse.valueOf($S))",
            argument.type(), argument.name(),
            argument.type(), Objects.class, argument.name());
      }
    }

    final String callArgs = arguments.stream()
        .map(Argument::name)
        .collect(Collectors.joining(", "));

    final MethodSpec create = createBuilder
        .addCode("\n")
        .addStatement("return $N.$N($L)", enclosingName, binding.name(), callArgs)
        .build();

    // public OptionParser parser() {
    final MethodSpec.Builder parserBuilder = methodBuilder("parser")
        .addModifiers(PUBLIC)
        .returns(OptionParser.class)
        .addStatement(
            "final $T parser = new $T()",
            OptionParser.class, OptionParser.class)
        .addCode("\n");

    // opt calls;
    for (Argument argument : arguments) {
      parserBuilder.addStatement(
          "opt($S, $T.class, parser)",
          argument.name(), argument.type());
    }

    parserBuilder
        .addCode("\n")
        .addStatement(
            "parser.acceptsAll($T.asList($S, $S)).forHelp()",
            Arrays.class, "h", "help")
        .addCode("\n")
        .addStatement("return parser");

    final MethodSpec parser = parserBuilder.build();

    return classBuilder(enclosingName + "_" + taskName)
        .addModifiers(PRIVATE, STATIC, FINAL)
        .addSuperinterface(TypeName.get(util.typeWithArgs(TaskConstructor.class, taskArg)))
        .addMethod(name)
        .addMethod(create)
        .addMethod(parser)
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
