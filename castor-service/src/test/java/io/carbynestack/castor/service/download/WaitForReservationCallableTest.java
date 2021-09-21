/*
 * Copyright (c) 2021 - for information on the respective copyright owner
 * see the NOTICE file and/or the repository https://github.com/carbynestack/castor.
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package io.carbynestack.castor.service.download;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.*;

import io.carbynestack.castor.common.entities.ActivationStatus;
import io.carbynestack.castor.common.entities.Reservation;
import io.carbynestack.castor.service.persistence.cache.ReservationCachingService;
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

  @Test
  public void givenNoReservationRetrievedAndCancelled_whenCall_thenReturnNull() {
    int retries = 1;
    WaitForReservationCallable wfrc =
        new WaitForReservationCallable(reservationId, reservationCachingServiceMock, 0);
    when(reservationCachingServiceMock.getReservation(reservationId))
        .thenAnswer(RunWhenAccessedAnswer.of(retries, wfrc::cancel, null));
    assertNull(wfrc.call());
    verify(reservationCachingServiceMock, times(retries)).getReservation(reservationId);
  }

  @Test
  public void givenReservationLockedInitially_whenCall_thenWaitUntilUnlockedAndReturnReservation() {
    int retries = 2;
    Reservation expectedReservation = mock(Reservation.class);
    WaitForReservationCallable wfrc =
        new WaitForReservationCallable(reservationId, reservationCachingServiceMock, 0);
    when(expectedReservation.getStatus()).thenReturn(ActivationStatus.LOCKED);
    when(reservationCachingServiceMock.getReservation(reservationId))
        .thenAnswer(
            RunWhenAccessedAnswer.of(
                retries,
                () -> when(expectedReservation.getStatus()).thenReturn(ActivationStatus.UNLOCKED),
                expectedReservation));
    assertEquals(expectedReservation, wfrc.call());
    verify(reservationCachingServiceMock, atLeast(2)).getReservation(reservationId);
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
