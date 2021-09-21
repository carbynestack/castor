/*
 * Copyright (c) 2021 - for information on the respective copyright owner
 * see the NOTICE file and/or the repository https://github.com/carbynestack/castor.
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package io.carbynestack.castor.common.entities;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.io.Serializable;
import java.util.List;
import lombok.AccessLevel;
import lombok.Data;
import lombok.NonNull;
import lombok.Setter;
import lombok.experimental.Accessors;

/**
 * A {@link Reservation} is used to manage {@link Tuple} consumption across all parties
 * participating in the CarbyneStack MPC-Cluster.
 *
 * <p><b>Info</b><br>
 * Castor operates in a so called master-slave setting. In order for methods that consume tuples for
 * their execution to produce valid results, it must be ensured that the consuming method receives
 * the matching shares of the same tuple from all parties in the cluster. This is ensured internally
 * by so-called reservations, which are administered on the part of the master service. As soon as
 * the master receives a request for tuples, it creates such a reservation and informs all other
 * parties in the cluster (slaves) which tuples are to be used for a specific request.
 */
@Data
@Setter(AccessLevel.NONE)
@Accessors(chain = true)
public class Reservation implements Serializable {
  private static final long serialVersionUID = -7176250403711227416L;
  public static final String ONE_RESERVATION_ELEMENT_REQUIRED_EXCEPTION_MSG =
      "At least one ReservationElement required.";
  public static final String ID_MUST_NOT_BE_NULL_OR_EMPTY_EXCEPTION_MSG =
      "ReservationId must not be empty.";

  /**
   * The identifier of the reservation used to link the reservation to a specific tuple request.
   * This id is shared across all parties in the CarbyneStack MPC-CLuster.
   */
  private final String reservationId;

  /** The Type of tuples that is reserved by this reservation. */
  private final TupleType tupleType;

  /** The {@link ReservationElement}s that are part of this {@link Reservation} */
  private final List<ReservationElement> reservations;

  /**
   * The status of the reservation indicating whether the referenced tuples can be consumed ({@link
   * ActivationStatus#UNLOCKED}) or not {@link ActivationStatus#LOCKED}.
   */
  @Setter private ActivationStatus status;

  /**
   * Creates a new {@link Reservation}
   *
   * @param reservationId Unique identifier of a reservation
   * @param tupleType Type of tuples that is reserved by this reservation
   * @param status The status of the reservation indicating whether the referenced tuples can be
   *     consumed
   * @param reservations The {@link ReservationElement}s that are part of this {@link Reservation}
   * @throws NullPointerException if any of the given arguments is <i>null</i>
   * @throws IllegalArgumentException if reservationId is empty
   * @throws IllegalArgumentException if reservations is empty
   */
  @JsonCreator
  protected Reservation(
      @NonNull @JsonProperty(value = "reservationId", required = true) String reservationId,
      @NonNull @JsonProperty(value = "tupleType", required = true) TupleType tupleType,
      @NonNull @JsonProperty(value = "status", required = true) ActivationStatus status,
      @NonNull @JsonProperty(value = "reservations", required = true)
          List<ReservationElement> reservations) {
    if (reservationId.isEmpty()) {
      throw new IllegalArgumentException(ID_MUST_NOT_BE_NULL_OR_EMPTY_EXCEPTION_MSG);
    }
    if (reservations.isEmpty()) {
      throw new IllegalArgumentException(ONE_RESERVATION_ELEMENT_REQUIRED_EXCEPTION_MSG);
    }
    this.reservationId = reservationId;
    this.tupleType = tupleType;
    this.status = status;
    this.reservations = reservations;
  }

  /**
   * Creates a new {@link Reservation} with default {@link #status} {@link ActivationStatus#LOCKED}
   *
   * @param reservationId Unique identifier of a reservation
   * @param tupleType Type of tuples that is reserved by this reservation
   * @param reservations The {@link ReservationElement}s that are part of this {@link Reservation}
   * @throws NullPointerException if any of the given arguments is <i>null</i>
   * @throws IllegalArgumentException if reservationId is empty
   * @throws IllegalArgumentException if reservations is empty
   */
  public Reservation(
      @NonNull String reservationId,
      @NonNull TupleType tupleType,
      @NonNull List<ReservationElement> reservations) {
    this(reservationId, tupleType, ActivationStatus.LOCKED, reservations);
  }
}
