/*
 * Copyright (c) 2021 - for information on the respective copyright owner
 * see the NOTICE file and/or the repository https://github.com/carbynestack/castor.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.carbynestack.castor.service.persistence.tuplestore;

import io.carbynestack.castor.common.entities.Field;
import io.carbynestack.castor.common.entities.Tuple;
import io.carbynestack.castor.common.entities.TupleChunk;
import io.carbynestack.castor.common.entities.TupleList;
import io.carbynestack.castor.common.exceptions.CastorServiceException;
import io.carbynestack.castor.service.config.MinioProperties;
import io.micrometer.core.annotation.Timed;
import io.minio.*;
import io.minio.errors.*;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.UUID;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.output.ByteArrayOutputStream;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.Assert;

/**
 * An implementation of the {@link TupleStore} interface backed by a Minio database to persist
 * secrets data.
 *
 * <p>The implementation will utilize the {@link MinioClient}'s functionality to efficiently upload,
 * store amd retrieve data. The data will therefore be processed as {@link ByteArrayInputStream}s
 * which allow for streaming large data chunks to the database.
 */
@Slf4j
@Service
public class MinioTupleStore implements TupleStore {
  public static final String PERSISTING_TUPLE_CHUNK_FAILED_EXCEPTION_MSG =
      "Persisting TupleChunk failed.";
  public static final String INVALID_INDEX_EXCEPTION_MSG =
      "Index of first byte to read must not be negative.";
  public static final String INVALID_LENGTH_EXCEPTION_MSG =
      "Must read at least one byte from database.";
  public static final String ERROR_WHILE_READING_TUPLES_EXCEPTION_MSG =
      "Error while reading tuples from database.";
  private final MinioClient minioClient;
  private final MinioProperties minioProperties;

  @Autowired
  public MinioTupleStore(MinioClient minioClient, MinioProperties minioProperties) {
    this.minioClient = minioClient;
    this.minioProperties = minioProperties;

    try {
      if (!minioClient.bucketExists(
          BucketExistsArgs.builder().bucket(minioProperties.getBucket()).build())) {
        minioClient.makeBucket(
            MakeBucketArgs.builder().bucket(minioProperties.getBucket()).build());
      }
    } catch (Exception e) {
      throw new CastorServiceException(e.getMessage(), e);
    }
  }

  @Override
  public void save(TupleChunk tupleChunk) {
    try (InputStream inputStream = new ByteArrayInputStream(tupleChunk.getTuples())) {
      minioClient.putObject(
          PutObjectArgs.builder()
              .bucket(minioProperties.getBucket())
              .object(tupleChunk.getChunkId().toString())
              .stream(inputStream, tupleChunk.getTuples().length, -1)
              .build());
    } catch (InvalidKeyException
        | IOException
        | ErrorResponseException
        | InsufficientDataException
        | InternalException
        | InvalidResponseException
        | NoSuchAlgorithmException
        | ServerException
        | XmlParserException e) {
      log.error("Exception occurred while writing tuple data to Minio.", e);
      throw new CastorServiceException(PERSISTING_TUPLE_CHUNK_FAILED_EXCEPTION_MSG, e);
    }
  }

  @Override
  public <T extends Tuple<T, F>, F extends Field> TupleList<T, F> downloadTuples(
      @NonNull Class<T> tupleCls,
      @NonNull F fieldType,
      @NonNull UUID tupleChunkId,
      long startIndex,
      long lengthToRead) {
    Assert.isTrue(startIndex >= 0, INVALID_INDEX_EXCEPTION_MSG);
    Assert.isTrue(lengthToRead >= 1, INVALID_LENGTH_EXCEPTION_MSG);

    log.debug(
        "Starting download from S3 for key {} from byte {} to byte {}",
        tupleChunkId,
        startIndex,
        startIndex + lengthToRead);

    try (InputStream byteData =
        minioClient.getObject(
            GetObjectArgs.builder()
                .bucket(minioProperties.getBucket())
                .object(tupleChunkId.toString())
                .offset(startIndex)
                .length(lengthToRead)
                .build())) {
      return TupleList.fromStream(tupleCls, fieldType, byteData, lengthToRead);
    } catch (Exception e) {
      log.error("Exception occurred while reading tuple data from Minio.", e);
      throw new CastorServiceException(ERROR_WHILE_READING_TUPLES_EXCEPTION_MSG, e);
    }
  }

  @Override
  public <T extends Tuple<T, F>, F extends Field> InputStream downloadTuplesAsBytes(
          @NonNull Class<T> tupleCls,
          @NonNull F fieldType,
          @NonNull UUID tupleChunkId,
          long startIndex,
          long lengthToRead) {
    Assert.isTrue(startIndex >= 0, INVALID_INDEX_EXCEPTION_MSG);
    Assert.isTrue(lengthToRead >= 1, INVALID_LENGTH_EXCEPTION_MSG);
    log.debug(
            "Starting download from S3 for key {} from byte {} to byte {} into byte array",
            tupleChunkId,
            startIndex,
            startIndex + lengthToRead);
  try{

      return minioClient.getObject(
              GetObjectArgs.builder()
                      .bucket(minioProperties.getBucket())
                      .object(tupleChunkId.toString())
                      .offset(startIndex)
                      .length(lengthToRead)
                      .build());
    } catch (Exception e) {
      log.error("Exception occurred while reading tuple data from Minio.", e);
      throw new CastorServiceException(ERROR_WHILE_READING_TUPLES_EXCEPTION_MSG, e);
    }
  }

  @Override
  public void deleteTupleChunk(UUID id) {
    try {
      minioClient.removeObject(
          RemoveObjectArgs.builder()
              .bucket(minioProperties.getBucket())
              .object(id.toString())
              .build());
    } catch (Exception e) {
      log.error("Exception occurred while deleting object from Minio", e);
    }
  }
}
