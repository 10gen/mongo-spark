package com.mongodb.spark.sql.connector.connection;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class LazyMongoClientCacheTest {

  private String legacyPropertyValue;
  private String propertyValue;

  @BeforeEach
  void saveSystemProperties() {
    legacyPropertyValue =
        System.getProperty(LazyMongoClientCache.LEGACY_SYSTEM_MONGO_CACHE_KEEP_ALIVE_MS_PROPERTY);
    propertyValue =
        System.getProperty(LazyMongoClientCache.SYSTEM_MONGO_CACHE_KEEP_ALIVE_MS_PROPERTY);
  }

  @AfterEach
  void restoreSystemProperties() {
    System.setProperty(
        LazyMongoClientCache.LEGACY_SYSTEM_MONGO_CACHE_KEEP_ALIVE_MS_PROPERTY,
        (legacyPropertyValue != null) ? legacyPropertyValue : "");
    System.setProperty(
        LazyMongoClientCache.SYSTEM_MONGO_CACHE_KEEP_ALIVE_MS_PROPERTY,
        (propertyValue != null) ? propertyValue : "");
  }

  @Test
  void computeKeepAliveUsesDefaultIfPropertiesNotSet() {
    System.setProperty(LazyMongoClientCache.LEGACY_SYSTEM_MONGO_CACHE_KEEP_ALIVE_MS_PROPERTY, "");
    System.setProperty(LazyMongoClientCache.SYSTEM_MONGO_CACHE_KEEP_ALIVE_MS_PROPERTY, "");

    assertEquals(5000, LazyMongoClientCache.computeKeepAlive(5000));
  }

  @Test
  void computeKeepAliveUsesLegacySystemProperty() {
    System.setProperty(
        LazyMongoClientCache.LEGACY_SYSTEM_MONGO_CACHE_KEEP_ALIVE_MS_PROPERTY, "123");
    System.setProperty(LazyMongoClientCache.SYSTEM_MONGO_CACHE_KEEP_ALIVE_MS_PROPERTY, "");

    assertEquals(123, LazyMongoClientCache.computeKeepAlive(5000));
  }

  @Test
  void computeKeepAliveUsesMongoSystemProperty() {
    System.setProperty(LazyMongoClientCache.LEGACY_SYSTEM_MONGO_CACHE_KEEP_ALIVE_MS_PROPERTY, "");
    System.setProperty(LazyMongoClientCache.SYSTEM_MONGO_CACHE_KEEP_ALIVE_MS_PROPERTY, "123");

    assertEquals(123, LazyMongoClientCache.computeKeepAlive(5000));
  }

  @Test
  void computeKeepAlivePrefersMongoSystemProperty() {
    System.setProperty(
        LazyMongoClientCache.LEGACY_SYSTEM_MONGO_CACHE_KEEP_ALIVE_MS_PROPERTY, "456");
    System.setProperty(LazyMongoClientCache.SYSTEM_MONGO_CACHE_KEEP_ALIVE_MS_PROPERTY, "123");

    assertEquals(123, LazyMongoClientCache.computeKeepAlive(5000));
  }
}
