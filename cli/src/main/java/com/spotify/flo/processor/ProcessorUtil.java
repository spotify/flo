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

import static java.util.Arrays.asList;
import static javax.tools.Diagnostic.Kind.NOTE;

import com.spotify.flo.Task;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.processing.Messager;
import javax.lang.model.element.Element;
import javax.lang.model.element.ElementKind;
import javax.lang.model.element.PackageElement;
import javax.lang.model.element.TypeElement;
import javax.lang.model.type.DeclaredType;
import javax.lang.model.type.TypeMirror;
import javax.lang.model.util.Elements;
import javax.lang.model.util.Types;

/**
 * Some handy methods for dealing with elements and types
 */
final class ProcessorUtil {

  final Types types;
  final Elements element;
  final Messager messager;

  ProcessorUtil(Types types, Elements element, Messager messager) {
    this.types = types;
    this.element = element;
    this.messager = messager;
  }

  PackageElement commonPackage(List<Binding> bindings) {
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
      final Map.Entry<String, RecursiveMap> next = node.entrySet().iterator().next();
      common += (common.isEmpty() ? "" : ".") + next.getKey();
      node = next.getValue();
    }
    return element.getPackageElement(common);
  }

  DeclaredType taskWildcard() {
    final TypeElement task = typeElement(Task.class);
    return types.getDeclaredType(task, types.getWildcardType(null, null));
  }

  DeclaredType mapStringString() {
    final TypeElement map = typeElement(Map.class);
    final TypeElement string = typeElement(String.class);
    return types.getDeclaredType(map, string.asType(), string.asType());
  }

  DeclaredType typeWithArgs(Class<?> clazz, Class<?>... args) {
    final TypeMirror[] typeArgs = asList(args).stream()
        .map(this::typeElement)
        .map(TypeElement::asType)
        .toArray(TypeMirror[]::new);
    return typeWithArgs(clazz, typeArgs);
  }

  DeclaredType typeWithArgs(Class<?> clazz, TypeMirror... args) {
    final TypeElement type = typeElement(clazz);
    return types.getDeclaredType(type, args);
  }

  TypeMirror refresh(TypeMirror typeMirror) {
    return element.getTypeElement(typeMirror.toString()).asType();
  }

  TypeMirror typeMirror(Class<?> clazz) {
    return typeElement(clazz).asType();
  }

  TypeElement typeElement(Class<?> clazz) {
    return element.getTypeElement(clazz.getCanonicalName());
  }

  PackageElement packageOf(Element element) {
    return this.element.getPackageOf(element);
  }

  TypeElement enclosingClass(Element element) {
    if (element.getKind() != ElementKind.CLASS) {
      return enclosingClass(element.getEnclosingElement());
    }

    return (TypeElement) element;
  }
}
