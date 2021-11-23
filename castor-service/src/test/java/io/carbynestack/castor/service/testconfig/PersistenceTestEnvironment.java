/*
 * Copyright (c) 2021 - for information on the respective copyright owner
 * see the NOTICE file and/or the repository https://github.com/carbynestack/castor.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.carbynestack.castor.service.testconfig;

import io.carbynestack.castor.common.entities.TupleType;
import io.carbynestack.castor.service.config.CastorCacheProperties;
import io.carbynestack.castor.service.config.MinioProperties;
import io.carbynestack.castor.service.persistence.markerstore.TupleChunkMetadataRepository;
import io.minio.*;
import io.minio.errors.*;
import io.vavr.control.Try;
import java.io.IOException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Objects;
import org.assertj.core.util.Lists;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cache.CacheManager;
import org.springframework.stereotype.Service;

@Service
public class PersistenceTestEnvironment {
  @Autowired private TupleChunkMetadataRepository tupleChunkMetadataRepository;

  @Autowired private CacheManager cacheManager;

  @Autowired private CastorCacheProperties cacheProperties;

  @Autowired private MinioClient minioClient;

  @Autowired private MinioProperties minioProperties;

  public void clearAllData() {
    try {
      tupleChunkMetadataRepository.deleteAll();
      Arrays.stream(TupleType.values())
          .forEach(
              tupleType ->
                  Objects.requireNonNull(
                          cacheManager.getCache(
                              cacheProperties.getConsumptionStorePrefix() + tupleType.toString()))
                      .clear());
      Objects.requireNonNull(cacheManager.getCache(cacheProperties.getReservationStore())).clear();
      if (minioClient.bucketExists(
          BucketExistsArgs.builder().bucket(minioProperties.getBucket()).build())) {
        Lists.newArrayList(
                minioClient
                    .listObjects(
                        ListObjectsArgs.builder().bucket(minioProperties.getBucket()).build())
                    .iterator())
            .forEach(
                itemResult ->
                    Try.run(
                        () ->
                            minioClient.removeObject(
                                RemoveObjectArgs.builder()
                                    .bucket(minioProperties.getBucket())
                                    .object(itemResult.get().objectName())
                                    .build())));
        minioClient.removeBucket(
            RemoveBucketArgs.builder().bucket(minioProperties.getBucket()).build());
      }
      minioClient.makeBucket(MakeBucketArgs.builder().bucket(minioProperties.getBucket()).build());
    } catch (ErrorResponseException
        | InsufficientDataException
        | InternalException
        | InvalidKeyException
        | InvalidResponseException
        | NoSuchAlgorithmException
        | ServerException
        | XmlParserException
        | IOException e) {
      throw new IllegalStateException("Failed clearing persisted data.", e);
    }
  }
}
