/*-
 * -\-\-
 * flo runner
 * --
 * Copyright (C) 2016 Spotify AB
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

package com.spotify.flo.util;

import com.google.auto.value.AutoValue;
import java.io.Serializable;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

/**
 * A value type representing a date. e.g. 1995-04-10, 2016-10-20,
 *
 * This type can be used for flo task arguments. It is compatible with the generated command line
 * parser from {@code flo-cli} by implementing {@link #valueOf(String)}.
 */
@AutoValue
public abstract class Date implements Comparable<Date>, Serializable {

  private static final long serialVersionUID = 1L;

  private static final DateTimeFormatter ISO_FRAGMENT_FMT =
      DateTimeFormatter.ofPattern("uuuu-MM-dd");

  public abstract LocalDate localDate();

  public static Date of(LocalDate date) {
    return new AutoValue_Date(date);
  }

  public static Date of(int year, int month, int day) {
    return of(LocalDate.of(year, month, day));
  }

  public static Date parse(String date) {
    return parse(date, ISO_FRAGMENT_FMT);
  }

  public static Date valueOf(String date) {
    return parse(date, ISO_FRAGMENT_FMT);
  }

  public static Date parse(String date, DateTimeFormatter formatter) {
    return of(formatter.parse(date, LocalDate::from));
  }

  public DateHour onHour(int hour) {
    return DateHour.of(this, hour);
  }

  public String format(DateTimeFormatter formatter) {
    return formatter.format(localDate());
  }

  @Override
  public String toString() {
    return ISO_FRAGMENT_FMT.format(localDate());
  }

  @Override
  public int compareTo(Date other) {
    return localDate().compareTo(other.localDate());
  }
}
