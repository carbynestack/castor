/*
 * Copyright (c) 2021 - for information on the respective copyright owner
 * see the NOTICE file and/or the repository https://github.com/carbynestack/castor.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.carbynestack.castor.client.download;

import io.carbynestack.castor.common.entities.ActivationStatus;
import io.carbynestack.castor.common.entities.Reservation;
import io.carbynestack.castor.common.exceptions.CastorClientException;

/**
 * Client interface for all MPC party internal Service-to-Service operations as used for managing
 * {@link Reservation}s.
 */
public interface CastorInterVcpClient {

  /**
   * Shares a {@link Reservation} with all (Castor) slave services.
   *
   * @param reservation the {@link Reservation} to share
   * @return <i>true</i> if the given {@link Reservation} was shared successfully, <i>false</i> if
   *     not
   * @throws CastorClientException if an communication error occurred
   */
  boolean shareReservation(Reservation reservation);

  /**
   * Updates the {@link Reservation#getStatus() status} for a {@link Reservation} with the given id.
   *
   * @param reservationId id of the referenced {@link Reservation}
   * @param status The new {@link ActivationStatus} to be set for the referenced {@link Reservation}
   * @throws CastorClientException if an communication error occurred
   */
  void updateReservationStatus(String reservationId, ActivationStatus status);
}
