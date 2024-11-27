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

package com.mongodb.spark.sql.connector.connection;

import com.mongodb.client.MongoClient;
import org.jetbrains.annotations.ApiStatus;

/**
 * A lazily initialized {@link MongoClientCache}.
 *
 * @see <a href="https://docs.oracle.com/javase/specs/jls/se8/html/jls-12.html#jls-12.4.1">Java
 *     class lazy initialization rules</a>
 */
@ApiStatus.Internal
public final class LazyMongoClientCache {

  private static final MongoClientCache CLIENT_CACHE;

  static final String LEGACY_SYSTEM_MONGO_CACHE_KEEP_ALIVE_MS_PROPERTY =
      "spark.mongodb.keep_alive_ms";

  static final String SYSTEM_MONGO_CACHE_KEEP_ALIVE_MS_PROPERTY = "mongodb.keep_alive_ms";

  static {
    int keepAliveMS = computeKeepAlive(5000);

    CLIENT_CACHE = new MongoClientCache(keepAliveMS);
  }

  static int computeKeepAlive(int defaultValue) {
    int keepAliveMS = defaultValue;
    try {
      String legacyKeepAliveMS =
          System.getProperty(LEGACY_SYSTEM_MONGO_CACHE_KEEP_ALIVE_MS_PROPERTY);
      if (legacyKeepAliveMS != null) {
        keepAliveMS = Integer.parseInt(legacyKeepAliveMS);
      }
    } catch (NumberFormatException e) {
      // ignore and use default
    }
    try {
      keepAliveMS = Integer.parseInt(System.getProperty(
          SYSTEM_MONGO_CACHE_KEEP_ALIVE_MS_PROPERTY, Integer.toString(defaultValue)));
    } catch (NumberFormatException e) {
      // ignore and use default
    }
    return keepAliveMS;
  }

  /**
   * Returns a {@link MongoClient} from the cache.
   *
   * @param mongoClientFactory the factory that is used to create the {@code MongoClient}
   * @return the MongoClient
   */
  public static MongoClient getMongoClient(final MongoClientFactory mongoClientFactory) {
    return CLIENT_CACHE.acquire(mongoClientFactory);
  }

  private LazyMongoClientCache() {}
}
