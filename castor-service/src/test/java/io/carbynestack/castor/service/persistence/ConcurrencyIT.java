/*
 * Copyright (c) 2022 - for information on the respective copyright owner
 * see the NOTICE file and/or the repository https://github.com/carbynestack/castor.
 *
 *  SPDX-License-Identifier: Apache-2.0
 */

package io.carbynestack.castor.service.persistence;

import static org.junit.Assert.assertEquals;
import static org.springframework.boot.test.context.SpringBootTest.WebEnvironment.RANDOM_PORT;

import io.carbynestack.castor.common.entities.ActivationStatus;
import io.carbynestack.castor.common.entities.Reservation;
import io.carbynestack.castor.common.entities.ReservationElement;
import io.carbynestack.castor.common.entities.TupleType;
import io.carbynestack.castor.service.CastorServiceApplication;
import io.carbynestack.castor.service.persistence.cache.ReservationCachingService;
import io.carbynestack.castor.service.persistence.fragmentstore.TupleChunkFragmentEntity;
import io.carbynestack.castor.service.persistence.fragmentstore.TupleChunkFragmentStorageService;
import io.carbynestack.castor.service.testconfig.PersistenceTestEnvironment;
import io.carbynestack.castor.service.testconfig.ReusableMinioContainer;
import io.carbynestack.castor.service.testconfig.ReusablePostgreSQLContainer;
import io.carbynestack.castor.service.testconfig.ReusableRedisContainer;
import io.vavr.control.Try;
import java.util.Collections;
import java.util.UUID;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest(
    webEnvironment = RANDOM_PORT,
    classes = {CastorServiceApplication.class})
@ActiveProfiles("test")
public class ConcurrencyIT {
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
  @Autowired private ReservationCachingService reservationCachingService;
  @Autowired private TupleChunkFragmentStorageService fragmentStorageService;

  private final TupleType testTupleType = TupleType.MULTIPLICATION_TRIPLE_GFP;
  private final UUID testChunkId = UUID.fromString("d1ea9d52-2b1d-42f0-85eb-e36caa6a623c");

  @Before
  public void setUp() {
    testEnvironment.clearAllData();
    long tuplesInChunk = 100;
    fragmentStorageService.keep(
        TupleChunkFragmentEntity.of(
            testChunkId, testTupleType, 0, tuplesInChunk, ActivationStatus.UNLOCKED, null));
    fragmentStorageService.activateFragmentsForTupleChunk(testChunkId);
  }

  @Test
  public void
      givenSubsequentReservationIsProcessedFirst_whenMasterSharesReservationOnParallelRequests_thenSucceedAsExpected() {
    long tuplesToReserve = 3;
    Reservation firstReservation =
        new Reservation(
            "1636913e-87b7-4331-97d7-4b14a1552604",
            testTupleType,
            Collections.singletonList(new ReservationElement(testChunkId, tuplesToReserve, 0)));
    Reservation secondReservation =
        new Reservation(
            "9b77009e-313e-4aa9-b0d0-da0235329139",
            testTupleType,
            Collections.singletonList(
                new ReservationElement(testChunkId, tuplesToReserve, tuplesToReserve)));
    reservationCachingService.keepReservation(secondReservation);
    assertEquals(
        Try.success(null),
        Try.run(() -> reservationCachingService.keepReservation(firstReservation)));
  }
}
