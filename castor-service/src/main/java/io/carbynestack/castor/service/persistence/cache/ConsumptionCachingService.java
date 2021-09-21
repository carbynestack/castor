/*
 * Copyright (c) 2021 - for information on the respective copyright owner
 * see the NOTICE file and/or the repository https://github.com/carbynestack/castor.
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package io.carbynestack.castor.service.persistence.cache;

import io.carbynestack.castor.common.entities.TupleType;
import io.carbynestack.castor.service.config.CastorCacheProperties;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.stream.Stream;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.cache.CacheKeyPrefix;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.ValueOperations;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class ConsumptionCachingService {

  private final RedisTemplate<String, Object> redisTemplate;
  private final CastorCacheProperties cacheProperties;

  @Autowired
  public ConsumptionCachingService(
      RedisTemplate<String, Object> redisTemplate, CastorCacheProperties cacheProperties) {
    this.redisTemplate = redisTemplate;
    this.cacheProperties = cacheProperties;
  }

  /**
   * Persists the consumption value for the given {@link TupleType} and timestamp in cache. This
   * method will automatically increment the consumption rate if an entry for the given timestamp
   * already exists.
   *
   * <p>This method will automatically set the time-to-life for the given data according to the
   * service configuration. (see {@link CastorCacheProperties#getTelemetryTtl()})
   *
   * @param timestamp The timestamp when the consumption happened. Must be a String for caching
   *     reasons, but must be convertible to a long value.
   * @param tupleType Defines which type of tuple was consumed
   * @param consumed Defines the number of consumed tuples
   */
  @Transactional
  public void keepConsumption(long timestamp, TupleType tupleType, long consumed) {
    ValueOperations<String, Object> ops = redisTemplate.opsForValue();
    String key = getCachePrefix(tupleType) + timestamp;
    Object currentValue = ops.get(key);
    if (currentValue != null) {
      ops.set(
          key,
          ((long) currentValue) + consumed,
          cacheProperties.getTelemetryTtl(),
          TimeUnit.SECONDS);
    } else {
      ops.set(key, consumed, cacheProperties.getTelemetryTtl(), TimeUnit.SECONDS);
    }
  }

  /**
   * Returns a sum of all consumptions for a specific tuple type
   *
   * @param timestamp The timestamp from which onwards the consumptions are returned
   * @param tupleType The type of tuple for which the consumptions are returned
   * @return The sum of all consumptions for the interval (timestamp until now)
   */
  @Transactional(readOnly = true)
  public long getConsumptionForTupleType(long timestamp, TupleType tupleType) {
    ValueOperations<String, Object> ops = redisTemplate.opsForValue();
    return getKeysMatchingTimeCondition(tupleType, key -> Long.parseLong(key) >= timestamp)
        .mapToLong(key -> (long) ops.get(key))
        .sum();
  }

  private Stream<String> getKeysMatchingTimeCondition(
      TupleType tupleType, Predicate<String> condition) {
    return redisTemplate.keys(getCachePrefix(tupleType) + "*").stream()
        .filter(redisKey -> condition.test(redisKey.substring(getCachePrefix(tupleType).length())));
  }

  private String getCachePrefix(TupleType tupleType) {
    return CacheKeyPrefix.simple()
        .compute(cacheProperties.getConsumptionStorePrefix() + tupleType.toString());
  }
}
