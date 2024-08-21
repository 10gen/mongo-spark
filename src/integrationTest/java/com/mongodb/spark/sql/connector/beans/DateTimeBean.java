/*
 * Copyright 2008-present MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package com.mongodb.spark.sql.connector.beans;

import java.io.Serializable;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Objects;

public class DateTimeBean implements Serializable {
  private java.sql.Date sqlDate;
  private java.sql.Timestamp sqlTimestamp;
  private java.time.LocalDate localDate;
  private java.time.Instant instant;
  private java.time.LocalDateTime localDateTime;

  public DateTimeBean() {}

  public DateTimeBean(
      final Date sqlDate,
      final Timestamp sqlTimestamp,
      final LocalDate localDate,
      final Instant instant,
      final LocalDateTime localDateTime
  ) {
    this.sqlDate = sqlDate;
    this.sqlTimestamp = sqlTimestamp;
    this.localDate = localDate;
    this.localDateTime = localDateTime;
    this.instant = instant;
  }

  public Date getSqlDate() {
    return sqlDate;
  }

  public void setSqlDate(final Date sqlDate) {
    this.sqlDate = sqlDate;
  }

  public Timestamp getSqlTimestamp() {
    return sqlTimestamp;
  }

  public void setSqlTimestamp(final Timestamp sqlTimestamp) {
    this.sqlTimestamp = sqlTimestamp;
  }

  public LocalDate getLocalDate() {
    return localDate;
  }

  public void setLocalDate(final LocalDate localDate) {
    this.localDate = localDate;
  }

  public LocalDateTime getLocalDateTime() {
    return localDateTime;
  }

  public void setLocalDateTime(final LocalDateTime localDateTime) {
    this.localDateTime = localDateTime;
  }

  public Instant getInstant() {
    return instant;
  }

  public void setInstant(final Instant instant) {
    this.instant = instant;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    DateTimeBean that = (DateTimeBean) o;
    return Objects.equals(sqlDate, that.sqlDate)
        && Objects.equals(sqlTimestamp, that.sqlTimestamp)
        && Objects.equals(localDate, that.localDate)
        && Objects.equals(localDateTime, that.localDateTime)
        && Objects.equals(instant, that.instant);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        sqlDate,
        sqlTimestamp,
        localDate,
        localDateTime,
        instant);
  }

  @Override
  public String toString() {
    return "DateTimeBean{"
        + "sqlDate="
        + sqlDate
        + ", sqlTimestamp="
        + sqlTimestamp
        + ", localDate="
        + localDate
        + ", localDateTime="
        + localDateTime
        + ", instant="
        + instant
        + '}';
  }
}
