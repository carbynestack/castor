/*
 * Copyright (c) 2021 - for information on the respective copyright owner
 * see the NOTICE file and/or the repository https://github.com/carbynestack/castor.
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package io.carbynestack.castor.service.persistence.markerstore;

import static io.carbynestack.castor.service.persistence.markerstore.TupleChunkMetaDataStorageService.CONFLICT_FOR_ID_EXCEPTION_MSG;
import static io.carbynestack.castor.service.persistence.markerstore.TupleChunkMetaDataStorageService.NO_METADATA_FOR_OD_EXCEPTION_MSG;
import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import io.carbynestack.castor.common.entities.ActivationStatus;
import io.carbynestack.castor.common.entities.TupleType;
import io.carbynestack.castor.common.exceptions.CastorClientException;
import java.util.Optional;
import java.util.UUID;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class TupleChunkMetaDataStorageServiceTest {
  @Mock private TupleChunkMetadataRepository tupleChunkMetadataRepositoryMock;

  @InjectMocks private TupleChunkMetaDataStorageService tupleChunkMetaDataStorageService;

  @Test
  public void
      givenAnEntityWithTheGivenIdIsAlreadyStoredInDatabase_whenKeepTupleChunkData_thenThrowCastorServiceException() {
    UUID chunkId = UUID.fromString("3fd7eaf7-cda3-4384-8d86-2c43450cbe63");

    when(tupleChunkMetadataRepositoryMock.existsById(chunkId)).thenReturn(true);

    CastorClientException actualCce =
        assertThrows(
            CastorClientException.class,
            () ->
                tupleChunkMetaDataStorageService.keepTupleChunkData(
                    chunkId, TupleType.MULTIPLICATION_TRIPLE_GFP, 42));

    assertEquals(String.format(CONFLICT_FOR_ID_EXCEPTION_MSG, chunkId), actualCce.getMessage());
  }

  @Test
  public void givenNoConflictForGivenId_whenKeepTupleChunkData_thenPersistExpectedContent() {
    UUID chunkId = UUID.fromString("3fd7eaf7-cda3-4384-8d86-2c43450cbe63");
    TupleType tupleType = TupleType.MULTIPLICATION_TRIPLE_GFP;
    long numberOfTuples = 42;

    when(tupleChunkMetadataRepositoryMock.existsById(chunkId)).thenReturn(false);
    when(tupleChunkMetadataRepositoryMock.save(any())).thenAnswer(i -> i.getArgument(0));

    TupleChunkMetaDataEntity actualMetaData =
        tupleChunkMetaDataStorageService.keepTupleChunkData(chunkId, tupleType, numberOfTuples);

    assertEquals(chunkId, actualMetaData.getTupleChunkId());
    assertEquals(tupleType, actualMetaData.getTupleType());
    assertEquals(numberOfTuples, actualMetaData.getNumberOfTuples());
    assertEquals(0, actualMetaData.getConsumedMarker());
    assertEquals(0, actualMetaData.getReservedMarker());
  }

  @Test
  public void givenEntityForIdInDatabase_whenGetTupleChunkData_thenReturnExpectedContent() {
    UUID chunkId = UUID.fromString("3fd7eaf7-cda3-4384-8d86-2c43450cbe63");
    TupleChunkMetaDataEntity metaDataEntityMock = mock(TupleChunkMetaDataEntity.class);

    when(tupleChunkMetadataRepositoryMock.findById(chunkId))
        .thenReturn(Optional.of(metaDataEntityMock));

    assertEquals(metaDataEntityMock, tupleChunkMetaDataStorageService.getTupleChunkData(chunkId));
  }

  @Test
  public void givenNoEntityForIdInDatabase_whenGetTupleChunkData_thenReturnNull() {
    UUID chunkId = UUID.fromString("3fd7eaf7-cda3-4384-8d86-2c43450cbe63");

    when(tupleChunkMetadataRepositoryMock.findById(chunkId)).thenReturn(Optional.empty());

    assertNull(tupleChunkMetaDataStorageService.getTupleChunkData(chunkId));
  }

  @Test
  public void
      givenNoEntityForIdInDatabase_updateReservationForTupleChunkData_thenThrowCastorServiceException() {
    UUID chunkId = UUID.fromString("3fd7eaf7-cda3-4384-8d86-2c43450cbe63");

    when(tupleChunkMetadataRepositoryMock.findById(chunkId)).thenReturn(Optional.empty());

    CastorClientException actualCce =
        assertThrows(
            CastorClientException.class,
            () -> tupleChunkMetaDataStorageService.updateReservationForTupleChunkData(chunkId, 42));

    assertEquals(String.format(NO_METADATA_FOR_OD_EXCEPTION_MSG, chunkId), actualCce.getMessage());
  }

  @Test
  public void
      givenEntityForIdInDatabase_updateReservationForTupleChunkData_thenUpdateReservationMarker() {
    UUID chunkId = UUID.fromString("3fd7eaf7-cda3-4384-8d86-2c43450cbe63");
    long additionalReservedTuples = 12;
    long currentReservedMarker = 30;
    TupleChunkMetaDataEntity metaDataEntityMock = mock(TupleChunkMetaDataEntity.class);

    when(tupleChunkMetadataRepositoryMock.findById(chunkId))
        .thenReturn(Optional.of(metaDataEntityMock));
    when(metaDataEntityMock.getReservedMarker()).thenReturn(currentReservedMarker);

    tupleChunkMetaDataStorageService.updateReservationForTupleChunkData(
        chunkId, additionalReservedTuples);

    verify(metaDataEntityMock).setReservedMarker(additionalReservedTuples + currentReservedMarker);
    verify(tupleChunkMetadataRepositoryMock).save(metaDataEntityMock);
  }

  @Test
  public void
      givenNoEntityForIdInDatabase_updateConsumptionForTupleChunkData_thenThrowCastorServiceException() {
    UUID chunkId = UUID.fromString("3fd7eaf7-cda3-4384-8d86-2c43450cbe63");

    when(tupleChunkMetadataRepositoryMock.findById(chunkId)).thenReturn(Optional.empty());

    CastorClientException actualCce =
        assertThrows(
            CastorClientException.class,
            () -> tupleChunkMetaDataStorageService.updateConsumptionForTupleChunkData(chunkId, 42));

    assertEquals(String.format(NO_METADATA_FOR_OD_EXCEPTION_MSG, chunkId), actualCce.getMessage());
  }

  @Test
  public void
      givenEntityForIdInDatabase_updateConsumptionForTupleChunkData_thenUpdateReservationMarker() {
    UUID chunkId = UUID.fromString("3fd7eaf7-cda3-4384-8d86-2c43450cbe63");
    long additionalConsumedTuples = 12;
    long currentConsumedMarker = 30;
    TupleChunkMetaDataEntity metaDataEntityMock = mock(TupleChunkMetaDataEntity.class);

    when(tupleChunkMetadataRepositoryMock.findById(chunkId))
        .thenReturn(Optional.of(metaDataEntityMock));
    when(metaDataEntityMock.getConsumedMarker()).thenReturn(currentConsumedMarker);

    tupleChunkMetaDataStorageService.updateConsumptionForTupleChunkData(
        chunkId, additionalConsumedTuples);

    verify(metaDataEntityMock).setConsumedMarker(additionalConsumedTuples + currentConsumedMarker);
    verify(tupleChunkMetadataRepositoryMock).save(metaDataEntityMock);
  }

  @Test
  public void givenNoEntityForIdInDatabase_activateTupleChunk_thenThrowCastorServiceException() {
    UUID chunkId = UUID.fromString("3fd7eaf7-cda3-4384-8d86-2c43450cbe63");

    when(tupleChunkMetadataRepositoryMock.findById(chunkId)).thenReturn(Optional.empty());

    CastorClientException actualCce =
        assertThrows(
            CastorClientException.class,
            () -> tupleChunkMetaDataStorageService.activateTupleChunk(chunkId));

    assertEquals(String.format(NO_METADATA_FOR_OD_EXCEPTION_MSG, chunkId), actualCce.getMessage());
  }

  @Test
  public void givenEntityForIdInDatabase_activateTupleChunk_thenSetStatusUnlockedAndPersist() {
    UUID chunkId = UUID.fromString("3fd7eaf7-cda3-4384-8d86-2c43450cbe63");
    TupleChunkMetaDataEntity metaDataEntityMock = mock(TupleChunkMetaDataEntity.class);

    when(tupleChunkMetadataRepositoryMock.findById(chunkId))
        .thenReturn(Optional.of(metaDataEntityMock));
    when(tupleChunkMetadataRepositoryMock.save(any())).thenAnswer(i -> i.getArgument(0));

    TupleChunkMetaDataEntity actualMetaData =
        tupleChunkMetaDataStorageService.activateTupleChunk(chunkId);

    assertEquals(metaDataEntityMock, actualMetaData);
    verify(metaDataEntityMock).setStatus(ActivationStatus.UNLOCKED);
  }

  @Test
  public void givenSuccessfulRequest_whenForgetTupleChunkData_thenCallDeleteOnDatabase() {
    UUID chunkId = UUID.fromString("3fd7eaf7-cda3-4384-8d86-2c43450cbe63");

    tupleChunkMetaDataStorageService.forgetTupleChunkData(chunkId);

    verify(tupleChunkMetadataRepositoryMock).deleteById(chunkId);
  }

  @Test
  public void
      givenTuplesForRequestedTypeAvailable_whenGetAvailableTuples_thenReturnExpectedContent() {
    TupleType tupleType = TupleType.MULTIPLICATION_TRIPLE_GFP;
    long expectedCount = 42;

    when(tupleChunkMetadataRepositoryMock.getAvailableTuplesByTupleType(tupleType))
        .thenReturn(expectedCount);

    assertEquals(expectedCount, tupleChunkMetaDataStorageService.getAvailableTuples(tupleType));
  }

  @Test
  public void givenRepositoryThrowsNullPointerException_whenGetAvailableTuples_thenReturnZero() {

    when(tupleChunkMetadataRepositoryMock.getAvailableTuplesByTupleType(any()))
        .thenThrow(new NullPointerException("expected"));

    assertEquals(
        0,
        tupleChunkMetaDataStorageService.getAvailableTuples(TupleType.MULTIPLICATION_TRIPLE_GFP));
  }
}
