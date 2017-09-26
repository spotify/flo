/*-
 * -\-\-
 * Flo Command Line Bindings
 * --
 * Copyright (C) 2016 - 2017 Spotify AB
 * --
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * -/-/-
 */

package com.spotify.flo.processor;

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
