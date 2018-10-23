/*-
 * -\-\-
 * hype-submitter
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

package com.spotify.flo.context;

import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.jhades.model.ClasspathEntry;
import org.jhades.service.ClasspathScanner;

class LocalClasspathInspector implements ClasspathInspector {

  private final ClasspathScanner scanner = new ClasspathScanner();
  private final ClassLoader classLoader;

  LocalClasspathInspector(ClassLoader classLoader) {
    this.classLoader = Objects.requireNonNull(classLoader);
  }

  LocalClasspathInspector(Class<?> clazz) {
    this.classLoader = Objects.requireNonNull(clazz).getClassLoader();
  }

  @Override
  public List<Path> classpathJars() {
    return localClasspath().stream()
        .map(entry -> Paths.get(URI.create(entry.getUrl())).toAbsolutePath())
        .collect(Collectors.toList());
  }

  private Set<ClasspathEntry> localClasspath() {
    final String classLoaderName = classLoader.getClass().getName();

    return scanner.findAllClasspathEntries().stream()
        .filter(entry -> classLoaderName.equals(entry.getClassLoaderName()))
        .flatMap(LocalClasspathInspector::jarFileEntriesWithExpandedManifest)
        .collect(Collectors.toCollection(LinkedHashSet::new));
  }

  private static Stream<ClasspathEntry> jarFileEntriesWithExpandedManifest(ClasspathEntry entry) {
    if ((!entry.isJar() && !entry.isClassFolder()) || !entry.getUrl().startsWith("file:")) {
      return Stream.empty();
    }

    if (entry.findManifestClasspathEntries().isEmpty()) {
      return Stream.of(entry);
    } else {
      final URI uri = URI.create(entry.getUrl());
      Path path = Paths.get(uri).getParent();
      return Stream.concat(
          Stream.of(entry),
          entry.findManifestClasspathEntries().stream()
              .map(normalizerUsingPath(path)));
    }
  }

  private static UnaryOperator<ClasspathEntry> normalizerUsingPath(Path base) {
    return entry -> new ClasspathEntry(
        entry.getClassLoader(),
        base.resolve(entry.getUrl()).toUri().toString());
  }
}
