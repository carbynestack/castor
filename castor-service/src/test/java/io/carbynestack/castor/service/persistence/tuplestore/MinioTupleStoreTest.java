/*
 * Copyright (c) 2021 - for information on the respective copyright owner
 * see the NOTICE file and/or the repository https://github.com/carbynestack/castor.
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package io.carbynestack.castor.service.persistence.tuplestore;

import static io.carbynestack.castor.common.entities.TupleType.INPUT_MASK_GFP;
import static io.carbynestack.castor.service.persistence.tuplestore.MinioTupleStore.*;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import io.carbynestack.castor.common.entities.*;
import io.carbynestack.castor.common.exceptions.CastorServiceException;
import io.carbynestack.castor.service.config.MinioProperties;
import io.minio.*;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.UUID;
import lombok.SneakyThrows;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.RandomUtils;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class MinioTupleStoreTest {
  @Mock private MinioClient minioClientMock;
  @Mock private MinioProperties minioPropertiesMock;

  private final String testBucketName = "test-bucket";

  private MinioTupleStore minioTupleStore;

  @SneakyThrows
  @Before
  public void setUp() {
    when(minioPropertiesMock.getBucket()).thenReturn(testBucketName);
    when(minioClientMock.bucketExists(BucketExistsArgs.builder().bucket(testBucketName).build()))
        .thenReturn(true);
    minioTupleStore = new MinioTupleStore(minioClientMock, minioPropertiesMock);
  }

  @SneakyThrows
  @Test
  public void givenBucketDoesNotExist_whenConstruct_thenMakeBucket() {
    when(minioClientMock.bucketExists(BucketExistsArgs.builder().bucket(testBucketName).build()))
        .thenReturn(false);
    new MinioTupleStore(minioClientMock, minioPropertiesMock);

    verify(minioClientMock).makeBucket(MakeBucketArgs.builder().bucket(testBucketName).build());
  }

  @SneakyThrows
  @Test
  public void givenSuccessfulRequest_whenSave_thenPutChunkDataInDatabase() {
    UUID chunkId = UUID.fromString("3fd7eaf7-cda3-4384-8d86-2c43450cbe63");
    int count = 1;
    byte[] chunkData = RandomUtils.nextBytes(INPUT_MASK_GFP.getTupleSize() * count);
    TupleChunk tupleChunkMock = mock(TupleChunk.class);

    when(tupleChunkMock.getChunkId()).thenReturn(chunkId);
    when(tupleChunkMock.getTuples()).thenReturn(chunkData);

    minioTupleStore.save(tupleChunkMock);

    ArgumentCaptor<PutObjectArgs> poaCaptor = ArgumentCaptor.forClass(PutObjectArgs.class);
    verify(minioClientMock).putObject(poaCaptor.capture());
    PutObjectArgs actualPoa = poaCaptor.getValue();
    assertEquals(testBucketName, actualPoa.bucket());
    assertEquals(chunkId.toString(), actualPoa.object());
    assertEquals("ByteArray", actualPoa.contentType());
    assertArrayEquals(chunkData, IOUtils.toByteArray(actualPoa.stream()));
  }

  @SneakyThrows
  @Test
  public void givenPutObjectThrowsException_whenSave_thenThrowCastorServiceException() {
    IOException expectedException = new IOException("expected");
    UUID chunkId = UUID.fromString("3fd7eaf7-cda3-4384-8d86-2c43450cbe63");
    int count = 1;
    byte[] chunkData = RandomUtils.nextBytes(INPUT_MASK_GFP.getTupleSize() * count);
    TupleChunk tupleChunkMock = mock(TupleChunk.class);
    ArgumentCaptor<PutObjectArgs> poaCaptor = ArgumentCaptor.forClass(PutObjectArgs.class);

    when(tupleChunkMock.getChunkId()).thenReturn(chunkId);
    when(tupleChunkMock.getTuples()).thenReturn(chunkData);
    doThrow(expectedException).when(minioClientMock).putObject(poaCaptor.capture());

    CastorServiceException actualCse =
        assertThrows(CastorServiceException.class, () -> minioTupleStore.save(tupleChunkMock));

    assertEquals(PERSISTING_TUPLE_CHUNK_FAILED_EXCEPTION_MSG, actualCse.getMessage());
    assertEquals(expectedException, actualCse.getCause());
    PutObjectArgs actualPoa = poaCaptor.getValue();
    assertEquals(testBucketName, actualPoa.bucket());
    assertEquals(chunkId.toString(), actualPoa.object());
    assertEquals("ByteArray", actualPoa.contentType());
    assertArrayEquals(chunkData, IOUtils.toByteArray(actualPoa.stream()));
  }

  @Test
  public void
      givenIndexOfFirstTupleToReadIsNegative_whenDownloadTuples_thenThrowIllegalArgumentException() {
    UUID chunkId = UUID.fromString("3fd7eaf7-cda3-4384-8d86-2c43450cbe63");
    TupleType tupleType = INPUT_MASK_GFP;
    long invalidIndex = -1;
    long length = 42;

    IllegalArgumentException actualIae =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                minioTupleStore.downloadTuples(
                    tupleType.getTupleCls(), tupleType.getField(), chunkId, invalidIndex, length));

    assertEquals(INVALID_INDEX_EXCEPTION_MSG, actualIae.getMessage());
  }

  @Test
  public void givenLengthToReadIsZero_whenDownloadTuples_thenThrowIllegalArgumentException() {
    UUID chunkId = UUID.fromString("3fd7eaf7-cda3-4384-8d86-2c43450cbe63");
    TupleType tupleType = INPUT_MASK_GFP;
    long startIndex = 0;
    long invalidLength = 0;

    IllegalArgumentException actualIae =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                minioTupleStore.downloadTuples(
                    tupleType.getTupleCls(),
                    tupleType.getField(),
                    chunkId,
                    startIndex,
                    invalidLength));

    assertEquals(INVALID_LENGTH_EXCEPTION_MSG, actualIae.getMessage());
  }

  @SneakyThrows
  @Test
  public void
      givenReadingFromDatabaseThrowsException_whenDownloadTuples_thenThrowCastorServiceException() {
    IOException expectedException = new IOException("expected");
    UUID chunkId = UUID.fromString("3fd7eaf7-cda3-4384-8d86-2c43450cbe63");
    TupleType tupleType = INPUT_MASK_GFP;
    long startIndex = 0;
    long length = 42;

    when(minioClientMock.getObject(
            GetObjectArgs.builder()
                .bucket(testBucketName)
                .object(chunkId.toString())
                .offset(startIndex)
                .length(length)
                .build()))
        .thenThrow(expectedException);

    CastorServiceException actualCse =
        assertThrows(
            CastorServiceException.class,
            () ->
                minioTupleStore.downloadTuples(
                    tupleType.getTupleCls(), tupleType.getField(), chunkId, startIndex, length));

    assertEquals(ERROR_WHILE_READING_TUPLES_EXCEPTION_MSG, actualCse.getMessage());
    assertEquals(expectedException, actualCse.getCause());
  }

  @SneakyThrows
  @Test
  public void givenSuccessfulRequest_whenDownloadTuples_thenThrowCastorServiceException() {
    UUID chunkId = UUID.fromString("3fd7eaf7-cda3-4384-8d86-2c43450cbe63");
    TupleType tupleType = INPUT_MASK_GFP;
    long startIndex = 0;
    long length = 42;
    byte[] chunkData = RandomUtils.nextBytes(INPUT_MASK_GFP.getTupleSize());

    when(minioClientMock.getObject(
            GetObjectArgs.builder()
                .bucket(testBucketName)
                .object(chunkId.toString())
                .offset(startIndex)
                .length(length)
                .build()))
        .thenReturn(
            new GetObjectResponse(
                null,
                testBucketName,
                null,
                chunkId.toString(),
                new ByteArrayInputStream(chunkData)));

    TupleList<InputMask<Field.Gfp>, Field.Gfp> actualTupleList =
        (TupleList<InputMask<Field.Gfp>, Field.Gfp>)
            minioTupleStore.downloadTuples(
                tupleType.getTupleCls(), tupleType.getField(), chunkId, startIndex, length);

    assertEquals(tupleType.getTupleCls(), actualTupleList.getTupleCls());
    assertEquals(tupleType.getField(), actualTupleList.getField());
    assertEquals(1, actualTupleList.size());
    assertArrayEquals(chunkData, actualTupleList.asChunk(chunkId).getTuples());
  }

  @SneakyThrows
  @Test
  public void givenNoChunkWithIdInDatabase_whenDeleteTupleChunk_thenDoNothing() {
    IOException expectedException = new IOException("expected");
    UUID chunkId = UUID.fromString("3fd7eaf7-cda3-4384-8d86-2c43450cbe63");

    doThrow(expectedException).when(minioClientMock).removeObject(any(RemoveObjectArgs.class));

    minioTupleStore.deleteTupleChunk(chunkId);

    verify(minioClientMock)
        .removeObject(
            RemoveObjectArgs.builder().bucket(testBucketName).object(chunkId.toString()).build());
  }
}
