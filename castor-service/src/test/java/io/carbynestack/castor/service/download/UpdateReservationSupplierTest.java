/*
 * Copyright (c) 2021 - for information on the respective copyright owner
 * see the NOTICE file and/or the repository https://github.com/carbynestack/castor.
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package io.carbynestack.castor.service.download;

import static io.carbynestack.castor.common.entities.TupleType.INPUT_MASK_GFP;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.carbynestack.castor.client.download.CastorInterVcpClient;
import io.carbynestack.castor.common.entities.ActivationStatus;
import io.carbynestack.castor.common.entities.Reservation;
import io.carbynestack.castor.common.entities.TupleType;
import io.carbynestack.castor.service.persistence.cache.ReservationCachingService;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class UpdateReservationSupplierTest {
  @Mock private CastorInterVcpClient castorInterVcpClientMock;
  @Mock private ReservationCachingService reservationCachingServiceMock;
  @Mock private Reservation reservationMock;

  @InjectMocks UpdateReservationSupplier updateReservationSupplier;

  @Test
  void givenSuccessfulRequest_whenGet_thenSetStatusToUnlockedAndPropagate() {
    UUID requestId = UUID.fromString("c8a0a467-16b0-4f03-b7d7-07cbe1b0e7e8");
    TupleType tupleType = INPUT_MASK_GFP;
    String reservationId = requestId + "_" + tupleType;
    when(reservationMock.getReservationId()).thenReturn(reservationId);
    when(reservationMock.getStatus()).thenReturn(ActivationStatus.UNLOCKED);

    assertEquals(reservationMock, updateReservationSupplier.get());

    verify(reservationMock).setStatus(ActivationStatus.UNLOCKED);
    verify(castorInterVcpClientMock)
        .updateReservationStatus(reservationId, ActivationStatus.UNLOCKED);
    verify(reservationCachingServiceMock)
        .updateReservation(reservationId, ActivationStatus.UNLOCKED);
  }
}
