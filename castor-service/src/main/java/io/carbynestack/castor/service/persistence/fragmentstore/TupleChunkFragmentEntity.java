/*
 * Copyright (c) 2022 - for information on the respective copyright owner
 * see the NOTICE file and/or the repository https://github.com/carbynestack/castor.
 *
 *  SPDX-License-Identifier: Apache-2.0
 */

package io.carbynestack.castor.service.persistence.fragmentstore;

import static io.carbynestack.castor.service.persistence.fragmentstore.TupleChunkFragmentEntity.TABLE_NAME;

import io.carbynestack.castor.common.entities.ActivationStatus;
import io.carbynestack.castor.common.entities.Tuple;
import io.carbynestack.castor.common.entities.TupleChunk;
import io.carbynestack.castor.common.entities.TupleType;
import java.io.Serializable;
import java.util.UUID;
import javax.persistence.*;
import lombok.*;
import lombok.experimental.Accessors;
import org.springframework.util.Assert;

/**
 * A {@link TupleChunkFragmentEntity} describes a unique sequence (or section) of {@link Tuple tuple
 * shares} within a referenced {@link TupleChunk} (see {@link #tupleChunkId}).<br>
 * While tuples that are referenced by a {@link TupleChunkFragmentEntity} are potentially available
 * for use or have already been reserved for a particular operation (see {@link #reservationId} or
 * {@link #activationStatus}), tuples which are no longer referenced by a {@link
 * TupleChunkFragmentEntity} are considered consumed. This means that a chunk has been completely
 * consumed once not a single of the contained tuples is referenced by a fragment anymore.
 */
@Getter
@ToString
@EqualsAndHashCode
@Accessors(chain = true)
@Entity
@Table(name = TABLE_NAME)
@AllArgsConstructor(access = AccessLevel.PROTECTED)
public class TupleChunkFragmentEntity implements Serializable {
  private static final long serialVersionUID = -6019183482286118231L;

  public static final String ID_MUST_NOT_BE_NULL_EXCEPTION_MSG = "TupleChunk ID must not be null.";
  public static final String TUPLE_TYPE_MUST_NOT_BE_NULL_EXCEPTION_MSG =
      "TupleType must not be null.";
  public static final String INVALID_START_INDEX_EXCEPTION_FORMAT =
      "Illegal start index (%d). (must be >= 0).";
  public static final String ILLEGAL_LAST_INDEX_EXCEPTION_FORMAT = "Illegal last index (%d).";

  static final String CLASS_NAME = "TupleChunkFragmentEntity";
  static final String TUPLE_CHUNK_ID_FIELD = "tupleChunkId";
  static final String START_INDEX_FIELD = "startIndex";
  static final String END_INDEX_FIELD = "endIndex";
  static final String TUPLE_TYPE_FIELD = "tupleType";
  static final String ACTIVATION_STATUS_FIELD = "activationStatus";
  static final String RESERVATION_ID_FIELD = "reservationId";
  static final String TABLE_NAME = "tuple_chunk_fragment";
  static final String FRAGMENT_ID_COLUMN = "fragment_id";
  static final String TUPLE_CHUNK_ID_COLUMN = "tuple_chunk_id";
  static final String TUPLE_TYPE_COLUMN = "tuple_type";
  static final String START_INDEX_COLUMN = "start_index";
  static final String END_INDEX_COLUMN = "end_index";
  static final String FRAGMENT_LENGTH_COLUMN = "fragment_length";
  static final String ACTIVATION_STATUS_COLUMN = "activation_status";
  static final String RESERVATION_ID_COLUMN = "reservation_id";
  static final String VIEW_NAME = "distributed_fragments";
  static final String POD_HASH_FIELD = "pod_hash";
  static final String POD_HASH = System.getenv("HOSTNAME");
  static final String TUPLECHUNK_STARTINDEX_INDEX_NAME = "chunk_startidx_idx";
  static final String IS_ROUND_COLUMN = "is_round";

  @Transient public static final String STATUS_COLUMN = "status";

  /** Unique identifier of this {@link TupleChunkFragmentEntity} */
  @Id
  @GeneratedValue(strategy = GenerationType.AUTO)
  @Column(name = FRAGMENT_ID_COLUMN)
  private Long id;

  /**
   * Unique identifier of the {@link TupleChunk} referenced by this {@link
   * TupleChunkFragmentEntity}.
   */
  @Column(name = TUPLE_CHUNK_ID_COLUMN)
  private final UUID tupleChunkId;

  /**
   * The type of tuples (see {@link TupleType}) described by this {@link TupleChunkFragmentEntity}
   */
  @Column(name = TUPLE_TYPE_COLUMN)
  @Enumerated(EnumType.STRING)
  private final TupleType tupleType;

  /**
   * The index of the first tuple within the {@link TupleChunk} referenced by this {@link
   * TupleChunkFragmentEntity}
   */
  @Column(name = START_INDEX_COLUMN)
  private final long startIndex;

  /**
   * The index of the last tuple (<b>exclusive</b>) of the {@link TupleChunk} referenced by this
   * {@link TupleChunkFragmentEntity}
   */
  private long endIndex;

  /**
   * Indicator whether the referenced {@link TupleChunk} is cleared for consumption. (see {@link
   * ActivationStatus})
   */
  @Setter
  @Column(name = ACTIVATION_STATUS_COLUMN)
  @Enumerated(EnumType.STRING)
  private ActivationStatus activationStatus;

  /**
   * The unique reservation identifier this {@link TupleChunkFragmentEntity} is reserved for /
   * related to.
   *
   * <p>While <code>null</code> indicates that the referenced tuples are available for consumption,
   * any {@link String} (even empty) will mark this {@link TupleChunkFragmentEntity} as reserved.
   */
  @Setter
  @Column(name = RESERVATION_ID_COLUMN)
  private String reservationId;

  @Setter
  @Column(name = IS_ROUND_COLUMN, columnDefinition = "BOOLEAN DEFAULT true")
  private boolean isRound;

  /** To be used by deserialization only */
  protected TupleChunkFragmentEntity() {
    this.tupleChunkId = null;
    this.tupleType = null;
    this.startIndex = 0;
  }

  /** Creates a new {@link TupleChunkFragmentEntity} according to the given parameters. */
  private TupleChunkFragmentEntity(
      UUID tupleChunkId,
      TupleType tupleType,
      long startIndex,
      long endIndex,
      ActivationStatus activationStatus,
      String reservationId,
      boolean isRound) {
    this.tupleChunkId = tupleChunkId;
    this.tupleType = tupleType;
    this.startIndex = startIndex;
    this.endIndex = endIndex;
    this.activationStatus = activationStatus;
    this.reservationId = reservationId;
    this.isRound = isRound;
  }

  /**
   * Sets the index, beginning with 0, of the last tuple within the {@link TupleChunk} referenced by
   * this {@link TupleChunkFragmentEntity}.
   *
   * @param endIndex The index of the last tuple of the sequence (<b>exclusive</b>) of the {@link
   *     TupleChunk}, beginning with 0.
   * @return This {@link TupleChunkFragmentEntity}.
   * @throws IllegalArgumentException if the endIndex is negative.
   */
  public TupleChunkFragmentEntity setEndIndex(long endIndex) {
    verifyEndIndex(this.startIndex, endIndex);
    this.endIndex = endIndex;
    return this;
  }

  /**
   * Returns a new {@link TupleChunkFragmentEntity} with the given configuration and {@link
   * #activationStatus} being set to {@link ActivationStatus#LOCKED} as well as {@link
   * #reservationId} set to <code>null</code>.
   *
   * @param tupleChunkId Unique identifier of the {@link TupleChunk} referenced by the new {@link
   *     TupleChunkFragmentEntity}.
   * @param tupleType The {@link TupleType} of the tuples referenced by the new {@link
   *     TupleChunkFragmentEntity}.
   * @param startIndex Index of the first tuple within the {@link TupleChunk} referenced by the new
   *     {@link TupleChunkFragmentEntity}.
   * @param endIndex Index of the last tuple (<b>exclusive</b>) of the {@link TupleChunk} referenced
   *     by this {@link TupleChunkFragmentEntity}
   * @return a new {@link TupleChunkFragmentEntity} created with the given configuration
   */
  public static TupleChunkFragmentEntity of(
      UUID tupleChunkId, TupleType tupleType, long startIndex, long endIndex) {
    return of(tupleChunkId, tupleType, startIndex, endIndex, ActivationStatus.LOCKED, null, true);
  }

  /**
   * Returns a new {@link TupleChunkFragmentEntity} with the given configuration.
   *
   * @param tupleChunkId Unique identifier of the {@link TupleChunk} referenced by the new {@link
   *     TupleChunkFragmentEntity}.
   * @param tupleType The {@link TupleType} of the tuples referenced by the new {@link
   *     TupleChunkFragmentEntity}.
   * @param startIndex Index of the first tuple within the {@link TupleChunk} referenced by the new
   *     {@link TupleChunkFragmentEntity}.
   * @param endIndex Index of the last tuple (<b>exclusive</b>) of the {@link TupleChunk} referenced
   *     by this {@link TupleChunkFragmentEntity}
   * @param activationStatus The {@link ActivationStatus} of the new {@link
   *     TupleChunkFragmentEntity} indicating whether the referenced tuples are available for
   *     consumption ({@link ActivationStatus#UNLOCKED}) or not ({@link ActivationStatus#LOCKED}).
   * @param reservationId The unique reservation identifier for the operation the new {@link
   *     TupleChunkFragmentEntity} is assigned to. Setting to <code>null</code> indi
   * @param isRound
   * @return a new {@link TupleChunkFragmentEntity} created with the given configuration
   */
  public static TupleChunkFragmentEntity of(
      UUID tupleChunkId,
      TupleType tupleType,
      long startIndex,
      long endIndex,
      ActivationStatus activationStatus,
      String reservationId,
      boolean isRound) {
    Assert.notNull(tupleChunkId, ID_MUST_NOT_BE_NULL_EXCEPTION_MSG);
    Assert.notNull(tupleType, TUPLE_TYPE_MUST_NOT_BE_NULL_EXCEPTION_MSG);
    verifyStartIndex(startIndex);
    verifyEndIndex(startIndex, endIndex);
    return new TupleChunkFragmentEntity(
        tupleChunkId, tupleType, startIndex, endIndex, activationStatus, reservationId, isRound);
  }

  /**
   * @throws IllegalArgumentException if startIndex < 0.
   */
  private static void verifyStartIndex(long startIndex) {
    if (startIndex < 0) {
      throw new IllegalArgumentException(
          String.format(INVALID_START_INDEX_EXCEPTION_FORMAT, startIndex));
    }
  }

  /**
   * @throws IllegalArgumentException if endIndex < startIndex.
   */
  private static void verifyEndIndex(long startIndex, long endIndex) {
    if (endIndex < startIndex) {
      throw new IllegalArgumentException(
          String.format(ILLEGAL_LAST_INDEX_EXCEPTION_FORMAT, endIndex));
    }
  }
}
