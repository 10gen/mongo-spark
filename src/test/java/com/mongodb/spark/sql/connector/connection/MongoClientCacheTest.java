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

import static com.mongodb.spark.sql.connector.config.MongoConfig.CONNECTION_STRING_CONFIG;
import static com.mongodb.spark.sql.connector.config.MongoConfig.DATABASE_NAME_CONFIG;
import static com.mongodb.spark.sql.connector.config.MongoConfig.PREFIX;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.mongodb.client.MongoClient;
import com.mongodb.spark.sql.connector.config.MongoConfig;
import com.mongodb.spark.sql.connector.config.ReadConfig;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class MongoClientCacheTest {
  private static final Map<String, String> CONFIG_MAP = new HashMap<>();

  @Mock
  private MongoClientFactory mongoClientFactory;

  @Mock
  private MongoClient mongoClient;

  static {
    CONFIG_MAP.put(PREFIX + CONNECTION_STRING_CONFIG, "mongodb://localhost:27017");
    CONFIG_MAP.put(PREFIX + DATABASE_NAME_CONFIG, "db");
  }

  @Test
  void testNormalUseCase() {
    MongoClientCache mongoClientCache = new MongoClientCache(0, 0, 100);
    when(mongoClientFactory.create()).thenReturn(mongoClient);

    // Create clients
    MongoClient client1 = mongoClientCache.acquire(mongoClientFactory);
    MongoClient client2 = mongoClientCache.acquire(mongoClientFactory);

    // Ensure only a single client was created
    verify(mongoClientFactory, times(1)).create();

    // Ensure calling client.close() eventually actually closes the underlying client
    client1.close();
    client2.close();
    sleep(200);
    verify(mongoClient, times(1)).close();
  }

  @Test
  void factoriesWithSameConfigCreateSameClientsThroughCache() {
    MongoConfig config = MongoConfig.createConfig(CONFIG_MAP);
    DefaultMongoClientFactory factory1 = new DefaultMongoClientFactory(config);
    DefaultMongoClientFactory factory2 = new DefaultMongoClientFactory(config);
    MongoClientCache mongoClientCache = new MongoClientCache(0, 0, 100);

    MongoClient client1 = mongoClientCache.acquire(factory1);
    MongoClient client2 = mongoClientCache.acquire(factory2);

    assertSame(client1, client2);
  }

  @Test
  void factoriesWithEqualReadConfigsCreateSameClientsThroughCache() {
    MongoConfig config1 = MongoConfig.readConfig(CONFIG_MAP);
    MongoConfig config2 = MongoConfig.readConfig(CONFIG_MAP);
    DefaultMongoClientFactory factory1 = new DefaultMongoClientFactory(config1);
    DefaultMongoClientFactory factory2 = new DefaultMongoClientFactory(config2);
    MongoClientCache mongoClientCache = new MongoClientCache(0, 0, 100);

    MongoClient client1 = mongoClientCache.acquire(factory1);
    MongoClient client2 = mongoClientCache.acquire(factory2);

    assertSame(client1, client2);
  }

  @Test
  void factoriesWithEqualWriteConfigsCreateNotSameClientsThroughCache() {
    MongoConfig config1 = MongoConfig.writeConfig(CONFIG_MAP);
    MongoConfig config2 = MongoConfig.writeConfig(CONFIG_MAP);
    DefaultMongoClientFactory factory1 = new DefaultMongoClientFactory(config1);
    DefaultMongoClientFactory factory2 = new DefaultMongoClientFactory(config2);
    MongoClientCache mongoClientCache = new MongoClientCache(0, 0, 100);

    MongoClient client1 = mongoClientCache.acquire(factory1);
    MongoClient client2 = mongoClientCache.acquire(factory2);

    assertSame(client1, client2);
  }

  @Test
  void factoriesWithEqualReadWriteConfigsCreateSameClientsThroughCache() {
    MongoConfig config1 = MongoConfig.readConfig(CONFIG_MAP);
    MongoConfig config2 = MongoConfig.writeConfig(CONFIG_MAP);
    DefaultMongoClientFactory factory1 = new DefaultMongoClientFactory(config1);
    DefaultMongoClientFactory factory2 = new DefaultMongoClientFactory(config2);
    MongoClientCache mongoClientCache = new MongoClientCache(0, 0, 100);

    MongoClient client1 = mongoClientCache.acquire(factory1);
    MongoClient client2 = mongoClientCache.acquire(factory2);

    assertSame(client1, client2);
  }

  @Test
  void factoriesWithSameSimpleConfigCreateSameClientsThroughCache() {
    MongoConfig config = MongoConfig.createConfig(CONFIG_MAP);
    DefaultMongoClientFactory factory1 = new DefaultMongoClientFactory(config);
    DefaultMongoClientFactory factory2 = new DefaultMongoClientFactory(config);
    MongoClientCache mongoClientCache = new MongoClientCache(0, 0, 100);

    MongoClient client1 = mongoClientCache.acquire(factory1);
    MongoClient client2 = mongoClientCache.acquire(factory2);

    assertSame(client1, client2);
  }

  @Test
  void factoriesWithEqualSimpleConfigsCreateSameClientsThroughCache() {
    MongoConfig config1 = MongoConfig.createConfig(CONFIG_MAP);
    MongoConfig config2 = MongoConfig.createConfig(CONFIG_MAP);
    DefaultMongoClientFactory factory1 = new DefaultMongoClientFactory(config1);
    DefaultMongoClientFactory factory2 = new DefaultMongoClientFactory(config2);
    MongoClientCache mongoClientCache = new MongoClientCache(0, 0, 100);

    MongoClient client1 = mongoClientCache.acquire(factory1);
    MongoClient client2 = mongoClientCache.acquire(factory2);

    assertSame(client1, client2);
  }

  @Test
  void factoriesWithDifferentAggregationPipelinesCreateSameClientsThroughCache() {
    MongoConfig config1 = MongoConfig.createConfig(CONFIG_MAP);
    Map<String, String> configMap2 = new HashMap<>(CONFIG_MAP);
    configMap2.put(ReadConfig.AGGREGATION_PIPELINE_CONFIG, "{ foo: \"bar\" }");
    MongoConfig config2 = MongoConfig.createConfig(configMap2);
    DefaultMongoClientFactory factory1 = new DefaultMongoClientFactory(config1);
    DefaultMongoClientFactory factory2 = new DefaultMongoClientFactory(config2);
    MongoClientCache mongoClientCache = new MongoClientCache(0, 0, 100);

    MongoClient client1 = mongoClientCache.acquire(factory1);
    MongoClient client2 = mongoClientCache.acquire(factory2);

    assertSame(client1, client2);
  }

  @Test
  void testKeepAliveReuseOfClient() {
    MongoClientCache mongoClientCache = new MongoClientCache(500, 0, 200);
    when(mongoClientFactory.create()).thenReturn(mongoClient);

    // Create clients
    MongoClient client1 = mongoClientCache.acquire(mongoClientFactory);
    MongoClient client2 = mongoClientCache.acquire(mongoClientFactory);

    // Ensure only a single client was created
    verify(mongoClientFactory, times(1)).create();

    // Ensure calling client.close() eventually actually closes the underlying client
    client1.close();
    client2.close();
    sleep(250);
    verify(mongoClient, times(0)).close();

    // Ensure acquiring a new MongoClient within the clean
    MongoClient client3 = mongoClientCache.acquire(mongoClientFactory);
    verify(mongoClientFactory, times(1)).create();
    verify(mongoClient, times(0)).close();

    // Ensure calling client.close() eventually actually closes the underlying client
    client3.close();
    sleep(1000);
    verify(mongoClient, times(1)).close();
  }

  @Test
  void testShutdown() {
    MongoClientCache mongoClientCache = new MongoClientCache(500, 0, 200);
    when(mongoClientFactory.create()).thenReturn(mongoClient);

    // Create clients
    MongoClient client1 = mongoClientCache.acquire(mongoClientFactory);
    MongoClient client2 = mongoClientCache.acquire(mongoClientFactory);

    verify(mongoClientFactory, times(1)).create();

    // shutdown
    mongoClientCache.shutdown();
    verify(mongoClient, times(1)).close();

    // Verify behaviour after shutdown
    assertDoesNotThrow(() -> mongoClientCache.acquire(mongoClientFactory));
    verify(mongoClientFactory, times(2)).create();
    assertDoesNotThrow(mongoClientCache::shutdown);
    verify(mongoClient, times(2)).close();
  }

  private void sleep(final long millis) {
    try {
      Thread.sleep(millis);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }
}
