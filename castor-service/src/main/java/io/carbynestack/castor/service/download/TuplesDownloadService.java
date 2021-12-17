/*
 * Copyright (c) 2021 - for information on the respective copyright owner
 * see the NOTICE file and/or the repository https://github.com/carbynestack/castor.
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package io.carbynestack.castor.service.download;

import io.carbynestack.castor.common.entities.Field;
import io.carbynestack.castor.common.entities.Reservation;
import io.carbynestack.castor.common.entities.Tuple;
import io.carbynestack.castor.common.entities.TupleList;
import io.carbynestack.castor.common.exceptions.CastorServiceException;
import io.carbynestack.castor.service.config.CastorSlaveServiceProperties;
import java.util.UUID;
import org.springframework.transaction.annotation.Transactional;

public interface TuplesDownloadService {

  /**
   * Returns the requested amount of {@link Tuple}s for the given class and {@link Field}.
   *
   * <p>This method behaves differently depending on whether the service is configured to run as
   * master or slave.
   *
   * <ul>
   *   <li/>Slave<br>
   *       The service will look for a reservation matching the given configuration or wait for a
   *       defined timeout if no reservation is yet available. As soon as the reservation is
   *       available, the tuples are retrieved accordingly and the corresponding number is marked as
   *       "consumed" in the corresponding chunk. Finally, the processed reservation is deleted and
   *       consumed chunks and their metadata are deleted.<br>
   *       In case of an exception, the following behaviour can be expected:
   *       <ul>
   *         <li/>No reservation available<br>
   *             No tuples are retrieved and consumed markers remain untouched.
   *         <li/>Retrieving tuples for a given reservation fails<br>
   *             Reservation remains in cache and consumed markers remain untouched.
   *         <li/>Consumed reservation cannot be deleted from cache<br>
   *             Reservation remains in cache and consumed markers remain untouched.
   *         <li/>Consumed chunks cannot be removed from database<br>
   *             Reservation remains in cache and consumed markers remain untouched.
   *       </ul>
   *   <li/>master<br>
   *       The service will look for a reservation matching the given configuration or create a new
   *       one accordingly. The new reservation is then shared with all defined slave services and
   *       activated for consumption on all parties. Creating and sharing the reservation will
   *       update the reservation markers for the referenced tuple chunks on all parties
   *       accordingly. Once the reservation is shared, the tuples are retrieved accordingly and the
   *       corresponding number is marked as "consumed" in the corresponding chunk. Finally, the
   *       processed reservation is deleted and consumed chunks and their metadata are deleted.<br>
   *       In case of an exception, the following behaviour can be expected:
   *       <ul>
   *         <li/>Reservation cannot be created<br>
   *             No tuples are retrieved and all data remains untouched.
   *         <li/>Reservation cannot be shared<br>
   *             No tuples are retrieved and all data remains untouched. Reservation is not
   *             persisted.
   *         <li/>Retrieving tuples for the given reservation fails<br>
   *             Reservation persisted in cache, reserved markers updated, telemetry invoked,
   *             consumed markers remain untouched.
   *         <li/>Consumed reservation cannot be deleted from cache<br>
   *             Reservation persisted in cache, reserved markers updated, telemetry invoked,
   *             consumed markers remain untouched.
   *         <li/>Consumed chunks cannot be removed from database<br>
   *             Reservation persisted in cache, reserved markers updated, telemetry invoked,
   *             consumed markers remain untouched.
   *       </ul>
   * </ul>
   *
   * @throws CastorServiceException if reservation was not shared successfully
   * @throws CastorServiceException if communication with slaves failed
   * @throws CastorServiceException if no reservation could be made for the given configuration
   * @throws CastorServiceException if no {@link Reservation} with the given id could be obtained
   *     within a defined timout (see {@link
   *     CastorSlaveServiceProperties#getWaitForReservationTimeout()}).
   * @throws CastorServiceException if tuples cannot be retrieved from database
   */
  @Transactional
  <T extends Tuple<T, F>, F extends Field> TupleList<T, F> getTupleList(
      Class<T> tupleCls, F field, long count, UUID requestId);
}
