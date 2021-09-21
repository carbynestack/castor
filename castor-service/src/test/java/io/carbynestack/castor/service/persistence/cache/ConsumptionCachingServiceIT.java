/*
 * Copyright (c) 2021 - for information on the respective copyright owner
 * see the NOTICE file and/or the repository https://github.com/carbynestack/castor.
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package io.carbynestack.castor.service.persistence.cache;

import static io.carbynestack.castor.common.entities.TupleType.INPUT_MASK_GFP;
import static io.carbynestack.castor.common.entities.TupleType.MULTIPLICATION_TRIPLE_GFP;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.when;
import static org.springframework.boot.test.context.SpringBootTest.WebEnvironment.RANDOM_PORT;

import io.carbynestack.castor.common.entities.TupleType;
import io.carbynestack.castor.service.CastorServiceApplication;
import io.carbynestack.castor.service.config.CastorCacheProperties;
import io.carbynestack.castor.service.testconfig.PersistenceTestEnvironment;
import io.carbynestack.castor.service.testconfig.ReusableMinioContainer;
import io.carbynestack.castor.service.testconfig.ReusablePostgreSQLContainer;
import io.carbynestack.castor.service.testconfig.ReusableRedisContainer;
import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import lombok.SneakyThrows;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.cache.Cache;
import org.springframework.cache.CacheManager;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest(
    webEnvironment = RANDOM_PORT,
    classes = {CastorServiceApplication.class})
@ActiveProfiles("test")
public class ConsumptionCachingServiceIT {

  @ClassRule
  public static ReusableRedisContainer reusableRedisContainer =
      ReusableRedisContainer.getInstance();

  @ClassRule
  public static ReusableMinioContainer reusableMinioContainer =
      ReusableMinioContainer.getInstance();

  @ClassRule
  public static ReusablePostgreSQLContainer reusablePostgreSQLContainer =
      ReusablePostgreSQLContainer.getInstance();

  @Autowired private PersistenceTestEnvironment testEnvironment;

  @SpyBean private CastorCacheProperties cachePropertiesSpy;

  @Autowired private CacheManager cacheManager;

  @Autowired private ConsumptionCachingService consumptionCachingService;

  @Before
  public void setUp() {
    testEnvironment.clearAllData();
  }

  @Test
  public void givenCacheIsEmpty_whenGetConsumption_thenReturnZero() {
    long actualConsumption =
        consumptionCachingService.getConsumptionForTupleType(0L, INPUT_MASK_GFP);
    assertEquals(0L, actualConsumption);
  }

  @Test
  public void
      givenMultipleConsumptionValues_whenGetConsumption_thenReturnExpectedOverallConsumptionForTimeFrame() {
    long timestamp1 = 100;
    long timestamp2 = 200;
    long consumption1 = 11;
    long consumption2 = 12;
    for (TupleType tupleType : TupleType.values()) {
      Cache tripleCache = getCacheForTupleType(tupleType);
      tripleCache.put(String.valueOf(timestamp1), consumption1);
      tripleCache.put(String.valueOf(timestamp2), consumption2);
    }

    long actualConsumption0ff =
        consumptionCachingService.getConsumptionForTupleType(0L, MULTIPLICATION_TRIPLE_GFP);
    long actualConsumption101ff =
        consumptionCachingService.getConsumptionForTupleType(
            timestamp1 + 1, MULTIPLICATION_TRIPLE_GFP);
    long actualConsumption201ff =
        consumptionCachingService.getConsumptionForTupleType(
            timestamp2 + 1, MULTIPLICATION_TRIPLE_GFP);

    assertEquals(consumption1 + consumption2, actualConsumption0ff);
    assertEquals(consumption2, actualConsumption101ff);
    assertEquals(0L, actualConsumption201ff);
  }

  @SneakyThrows
  @Test
  public void givenTimeToLife_whenKeepConsumption_thenRemoveValueAutomaticallyFromCache() {
    long time = 100;
    long ttl = 1;
    long consumed = 42;
    when(cachePropertiesSpy.getTelemetryTtl()).thenReturn(ttl);
    consumptionCachingService.keepConsumption(time, MULTIPLICATION_TRIPLE_GFP, consumed);
    TimeUnit.SECONDS.sleep(ttl * 3);
    assertNull(getCacheForTupleType(MULTIPLICATION_TRIPLE_GFP).get(time, Long.class));
  }

  @Test
  public void givenMultipleConsumptionValuesForSameTimestamp_whenKeepConsumption_thenSumUpValues() {
    long timestamp = 100L;
    long[] consumptionValues = {1, 12, 23, 34, 42};
    long expectedTotal = Arrays.stream(consumptionValues).sum();

    for (long consumptionValue : consumptionValues) {
      consumptionCachingService.keepConsumption(
          timestamp, MULTIPLICATION_TRIPLE_GFP, consumptionValue);
    }

    Cache tripleCache = getCacheForTupleType(MULTIPLICATION_TRIPLE_GFP);
    long actualConsumption = Objects.requireNonNull(tripleCache.get(timestamp, Long.class));

    assertEquals(expectedTotal, actualConsumption);
  }

  private Cache getCacheForTupleType(TupleType tupleType) {
    return cacheManager.getCache(cachePropertiesSpy.getConsumptionStorePrefix() + tupleType);
  }
}
