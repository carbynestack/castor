/*
 * Copyright (c) 2021 - for information on the respective copyright owner
 * see the NOTICE file and/or the repository https://github.com/carbynestack/castor.
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package io.carbynestack.castor.service.persistence.markerstore;

import static io.carbynestack.castor.common.entities.ActivationStatus.UNLOCKED;
import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.springframework.boot.test.context.SpringBootTest.WebEnvironment.RANDOM_PORT;

import io.carbynestack.castor.common.entities.TupleType;
import io.carbynestack.castor.service.CastorServiceApplication;
import io.carbynestack.castor.service.testconfig.PersistenceTestEnvironment;
import io.carbynestack.castor.service.testconfig.ReusableMinioContainerExtension;
import io.carbynestack.castor.service.testconfig.ReusablePostgreSQLContainerExtension;
import io.carbynestack.castor.service.testconfig.ReusableRedisContainerExtension;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

@SpringBootTest(
    webEnvironment = RANDOM_PORT,
    classes = {CastorServiceApplication.class})
@ActiveProfiles("test")
class TupleChunkMetaDataStorageServiceIT {

  @RegisterExtension
  public static ReusableRedisContainerExtension reusableRedisContainer =
      ReusableRedisContainerExtension.getInstance();

  @RegisterExtension
  public static ReusableMinioContainerExtension reusableMinioContainer =
      ReusableMinioContainerExtension.getInstance();

  @RegisterExtension
  public static ReusablePostgreSQLContainerExtension reusablePostgreSQLContainer =
      ReusablePostgreSQLContainerExtension.getInstance();

  @Autowired private PersistenceTestEnvironment testEnvironment;

  @Autowired TupleChunkMetadataRepository tupleChunkMetadataRepository;

  @Autowired private TupleChunkMetaDataStorageService markerStore;

  private final TupleType testTupleType = TupleType.MULTIPLICATION_TRIPLE_GFP;

  private final TupleChunkMetaDataEntity testLockedTupleChunkMetaDataEntity =
      TupleChunkMetaDataEntity.of(
          UUID.fromString("3fd7eaf7-cda3-4384-8d86-2c43450cbe63"), testTupleType, 24);

  private final TupleChunkMetaDataEntity testUnlockedTupleChunkMetaDataEntity =
      TupleChunkMetaDataEntity.of(
              UUID.fromString("95e9f39a-a863-4ad2-9b41-07a8b1bf4763"), testTupleType, 42)
          .setStatus(UNLOCKED);

  @BeforeEach
  public void setUp() {
    testEnvironment.clearAllData();
  }

  @Test
  void givenNoEntityWithIdInDatabase_whenGetTupleChunkData_thenReturnNull() {
    UUID chunkId = UUID.fromString("9463b2ac-e865-4934-8f0b-6cdf19dd86fd");

    assertNull(markerStore.getTupleChunkData(chunkId));
  }

  @Test
  void
      givenLockedAndUnlockedEntitiesInDatabase_whenGetTupleChunkData_thenReturnUnlockedContentOnly() {
    tupleChunkMetadataRepository.save(testLockedTupleChunkMetaDataEntity);
    tupleChunkMetadataRepository.save(testUnlockedTupleChunkMetaDataEntity);
    assertEquals(
        singletonList(testUnlockedTupleChunkMetaDataEntity),
        markerStore.getTupleChunkData(testTupleType));
  }

  @Test
  void givenTuplesForRequestedTypeAvailable_whenGetAvailableTuples_thenReturnExpectedContent() {
    tupleChunkMetadataRepository.save(testUnlockedTupleChunkMetaDataEntity);

    long actualAvailableTuples = markerStore.getAvailableTuples(testTupleType);
    assertEquals(testUnlockedTupleChunkMetaDataEntity.getNumberOfTuples(), actualAvailableTuples);
  }

  @Test
  void givenOnlyLockedChunksAvailable_whenGetAvailableTuples_thenReturnZero() {
    tupleChunkMetadataRepository.save(testLockedTupleChunkMetaDataEntity);

    long actualAvailableTuples = markerStore.getAvailableTuples(testTupleType);

    assertEquals(0, actualAvailableTuples);
  }
}
