/*
 * Copyright (c) 2021 - for information on the respective copyright owner
 * see the NOTICE file and/or the repository https://github.com/carbynestack/castor.
 *
 *  SPDX-License-Identifier: Apache-2.0
 */

package io.carbynestack.castor.service.persistence.cache;

import static io.carbynestack.castor.common.entities.TupleType.MULTIPLICATION_TRIPLE_GFP;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.*;

import io.carbynestack.castor.common.entities.Reservation;
import io.carbynestack.castor.common.entities.TupleType;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.junit.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;

@Slf4j
@RunWith(MockitoJUnitRunner.class)
public class WaitForReservationCallableTest {
  @Mock private ReservationCachingService reservationCachingServiceMock;

  private final String reservationId = "reservationId";
  private final TupleType tupleType = MULTIPLICATION_TRIPLE_GFP;
  private final long tupleCount = 42;

  @Test
  public void givenNoReservationRetrievedAndCancelled_whenCall_thenReturnNull() {
    int retries = 1;
    WaitForReservationCallable wfrc =
        new WaitForReservationCallable(
            reservationId, tupleType, tupleCount, reservationCachingServiceMock, 0);
    when(reservationCachingServiceMock.getUnlockedReservation(reservationId, tupleType, tupleCount))
        .thenAnswer(RunWhenAccessedAnswer.of(retries, wfrc::cancel, null));
    assertNull(wfrc.call());
    verify(reservationCachingServiceMock, times(1))
        .getUnlockedReservation(reservationId, tupleType, tupleCount);
  }

  @Test
  public void givenReservationLockedInitially_whenCall_thenWaitUntilUnlockedAndReturnReservation() {
    Reservation expectedReservation = mock(Reservation.class);
    WaitForReservationCallable wfrc =
        new WaitForReservationCallable(
            reservationId, tupleType, tupleCount, reservationCachingServiceMock, 0);
    when(reservationCachingServiceMock.getUnlockedReservation(reservationId, tupleType, tupleCount))
        .thenReturn(null)
        .thenReturn(expectedReservation);
    assertEquals(expectedReservation, wfrc.call());
    verify(reservationCachingServiceMock, times(2))
        .getUnlockedReservation(reservationId, tupleType, tupleCount);
  }

  @RequiredArgsConstructor(staticName = "of")
  private static final class RunWhenAccessedAnswer implements Answer<Object> {
    private final Object $lock = new Object[0];
    private int callCount = 0;
    private final int delay;
    private final Runnable runnable;
    private final Object defaultResult;

    @Override
    public Object answer(InvocationOnMock invocation) {
      synchronized ($lock) {
        if (++callCount >= delay) {
          runnable.run();
        }
        return defaultResult;
      }
    }
  }
}
