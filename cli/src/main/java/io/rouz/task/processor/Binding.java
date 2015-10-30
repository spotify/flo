package io.rouz.task.processor;

import com.google.auto.value.AutoValue;

import java.util.Map;

import javax.lang.model.element.Name;
import javax.lang.model.type.TypeMirror;

/**
 * TODO: document.
 */
@AutoValue
abstract class Binding {

  abstract Name name();

  abstract Map<Name, TypeMirror> arguments();
}
