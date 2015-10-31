package io.rouz.task.processor;

import com.google.auto.value.AutoValue;

import java.util.List;

import javax.lang.model.element.ExecutableElement;
import javax.lang.model.element.Name;
import javax.lang.model.element.TypeElement;
import javax.lang.model.type.TypeMirror;

/**
 * TODO: document.
 */
@AutoValue
abstract class Binding {

  abstract ExecutableElement method();

  abstract TypeElement enclosingClass();

  abstract TypeMirror returnType();

  abstract Name name();

  abstract List<Argument> arguments();

  @AutoValue
  static abstract class Argument {

    abstract Name name();

    abstract TypeMirror type();
  }

  static Binding create(
      ExecutableElement method,
      TypeElement enclosingClass,
      TypeMirror returnType,
      Name name,
      List<Argument> arguments) {
    return new AutoValue_Binding(method, enclosingClass, returnType, name, arguments);
  }

  static Argument argument(Name name, TypeMirror type) {
    return new AutoValue_Binding_Argument(name, type);
  }
}
