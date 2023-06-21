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
package com.mongodb.spark.sql.connector.read;

import org.bson.BsonDocument;
import org.bson.BsonTimestamp;

import com.mongodb.client.ChangeStreamIterable;

final class BsonTimestampOffset extends MongoOffset {
  private static final long serialVersionUID = 1L;
  private final long bsonTimestampValue;
  private transient BsonTimestamp bsonTimestamp;

  BsonTimestampOffset(final BsonTimestamp value) {
    bsonTimestamp = value;
    bsonTimestampValue = value.getValue();
  }

  BsonTimestampOffset(final long value) {
    this.bsonTimestampValue = value;
  }

  @Override
  String getOffsetJsonValue() {
    String docJson =
        new BsonDocument("v", getBsonTimestamp()).toJson(EXTENDED_JSON_WRITER_SETTINGS);
    return docJson.substring(6, docJson.length() - 1);
  }

  @Override
  <T> ChangeStreamIterable<T> applyToChangeStreamIterable(
      final ChangeStreamIterable<T> changeStreamIterable) {
    if (getBsonTimestamp().getTime() >= 0) {
      return changeStreamIterable.startAtOperationTime(getBsonTimestamp());
    }
    return changeStreamIterable;
  }

  BsonTimestamp getBsonTimestamp() {
    if (bsonTimestamp == null) {
      bsonTimestamp = new BsonTimestamp(bsonTimestampValue);
    }
    return bsonTimestamp;
  }
}
