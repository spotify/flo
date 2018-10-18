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

import static java.time.ZoneOffset.UTC;

import com.google.auto.value.AutoValue;
import java.io.Serializable;
import java.time.LocalDate;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;

/**
 * A value type representing a localDate and an hour in the UTC time-zone.
 *
 * This type can be used for flo task arguments. It is compatible with the generated command line
 * parser from {@code flo-cli} by implementing {@link #valueOf(String)}.
 */
@AutoValue
public abstract class DateHour implements Comparable<DateHour>, Serializable {

  private static final long serialVersionUID = 1L;

  private static final DateTimeFormatter ISO_FRAGMENT_FMT =
      DateTimeFormatter.ofPattern("uuuu-MM-dd'T'HH")
          .withZone(UTC);

  public abstract ZonedDateTime dateTime();

  public static DateHour of(ZonedDateTime dateHour) {
    if (!dateHour.equals(dateHour.truncatedTo(ChronoUnit.HOURS))) {
      throw new IllegalArgumentException("dateHour should be truncated to the hour");
    }
    if (!dateHour.getZone().equals(UTC)) {
      throw new IllegalArgumentException("dateHour should be in UTC");
    }
    return new AutoValue_DateHour(dateHour);
  }

  public static DateHour of(int year, int month, int day, int hour) {
    return of(ZonedDateTime.of(year, month, day, hour, 0, 0, 0, UTC));
  }

  public static DateHour of(LocalDate date, int hour) {
    return of(date.atStartOfDay(UTC).withHour(hour));
  }

  public static DateHour of(Date date, int hour) {
    return of(date.localDate().atStartOfDay(UTC).withHour(hour));
  }

  public static DateHour parse(String dateHour) {
    return parse(dateHour, ISO_FRAGMENT_FMT);
  }

  public static DateHour valueOf(String dateHour) {
    return parse(dateHour, ISO_FRAGMENT_FMT);
  }

  public static DateHour parse(String dateHour, DateTimeFormatter formatter) {
    return of(formatter.parse(dateHour, ZonedDateTime::from));
  }

  public Date date() {
    return Date.of(dateTime().toLocalDate());
  }

  public int hour() {
    return dateTime().getHour();
  }

  public String format(DateTimeFormatter formatter) {
    return formatter.format(dateTime());
  }

  @Override
  public String toString() {
    return ISO_FRAGMENT_FMT.format(dateTime());
  }

  @Override
  public int compareTo(DateHour other) {
    return dateTime().compareTo(other.dateTime());
  }
}
