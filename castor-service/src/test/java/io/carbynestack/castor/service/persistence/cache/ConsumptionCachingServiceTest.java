/*
 * Copyright (c) 2021 - for information on the respective copyright owner
 * see the NOTICE file and/or the repository https://github.com/carbynestack/castor.
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package io.carbynestack.castor.service.persistence.cache;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.carbynestack.castor.common.entities.TupleType;
import io.carbynestack.castor.service.config.CastorCacheProperties;
import io.vavr.Tuple2;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.redis.cache.CacheKeyPrefix;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.ValueOperations;

@ExtendWith(MockitoExtension.class)
class ConsumptionCachingServiceTest {
  @Mock private RedisTemplate<String, Object> redisTemplateMock;
  @Mock private CastorCacheProperties castorCachePropertiesMock;
  @Mock private ValueOperations<String, Object> valueOperationsMock;

  @InjectMocks private ConsumptionCachingService consumptionCachingService;

  @BeforeEach
  public void setUp() {
    when(redisTemplateMock.opsForValue()).thenReturn(valueOperationsMock);
  }

  @Test
  void givenNoValueForTimeStampInCache_whenKeepConsumption_thenPersistGivenValue() {
    long time = 100L;
    TupleType tupleType = TupleType.INPUT_MASK_GFP;
    long consumed = 42;
    String cachePrefix = "testCache_";
    long ttl = 100;
    String expectedKey = CacheKeyPrefix.simple().compute(cachePrefix + tupleType) + time;

    when(castorCachePropertiesMock.getConsumptionStorePrefix()).thenReturn(cachePrefix);
    when(castorCachePropertiesMock.getTelemetryTtl()).thenReturn(ttl);
    when(valueOperationsMock.get(expectedKey)).thenReturn(null);

    consumptionCachingService.keepConsumption(time, tupleType, consumed);

    verify(valueOperationsMock).set(expectedKey, consumed, ttl, TimeUnit.SECONDS);
  }

  @Test
  void givenValueForSameTimeInCache_whenKeepConsumption_thenPersistGivenValue() {
    long time = 100L;
    TupleType tupleType = TupleType.INPUT_MASK_GFP;
    long consumedNew = 42;
    long consumedExisting = 42;
    String cachePrefix = "testCache_";
    long ttl = 100;
    String expectedKey = CacheKeyPrefix.simple().compute(cachePrefix + tupleType) + time;

    when(castorCachePropertiesMock.getConsumptionStorePrefix()).thenReturn(cachePrefix);
    when(castorCachePropertiesMock.getTelemetryTtl()).thenReturn(ttl);
    when(valueOperationsMock.get(expectedKey)).thenReturn(consumedExisting);

    consumptionCachingService.keepConsumption(time, tupleType, consumedNew);

    verify(valueOperationsMock)
        .set(expectedKey, consumedNew + consumedExisting, ttl, TimeUnit.SECONDS);
  }

  @Test
  void
      givenTimeStamp_whenGetConsumptionForTupleType_thenFilterAccordinglyAndReturnAggregatedValue() {
    String cachePrefix = "testCache_";
    TupleType tupleType = TupleType.INPUT_MASK_GFP;
    String keyPrefix = CacheKeyPrefix.simple().compute(cachePrefix + tupleType);
    long time = 100;
    List<Tuple2<String, Long>> timeFrame1 =
        Arrays.asList(
            new Tuple2<>(keyPrefix + (time - 20), 12L), new Tuple2<>(keyPrefix + (time - 10), 97L));
    List<Tuple2<String, Long>> timeFrame2 =
        Arrays.asList(
            new Tuple2<>(keyPrefix + (time + 10), 27L), new Tuple2<>(keyPrefix + (time + 20), 15L));
    long expectedConsumptionFrame2 = timeFrame2.stream().mapToLong(Tuple2::_2).sum();
    long expectedConsumptionFrame1 =
        timeFrame1.stream().mapToLong(Tuple2::_2).sum() + expectedConsumptionFrame2;
    List<String> allKeys = new ArrayList<>();
    allKeys.addAll(timeFrame1.stream().map(Tuple2::_1).collect(Collectors.toList()));
    allKeys.addAll(timeFrame2.stream().map(Tuple2::_1).collect(Collectors.toList()));

    when(castorCachePropertiesMock.getConsumptionStorePrefix()).thenReturn(cachePrefix);
    when(redisTemplateMock.keys(any())).thenReturn(new HashSet<>(allKeys));
    for (Tuple2<String, Long> value : timeFrame1) {
      when(valueOperationsMock.get(value._1)).thenReturn(value._2);
    }
    for (Tuple2<String, Long> value : timeFrame2) {
      when(valueOperationsMock.get(value._1)).thenReturn(value._2);
    }

    assertEquals(
        expectedConsumptionFrame1,
        consumptionCachingService.getConsumptionForTupleType(0, tupleType));
    assertEquals(
        expectedConsumptionFrame2,
        consumptionCachingService.getConsumptionForTupleType(time, tupleType));
  }
}
