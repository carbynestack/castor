/*
 * Copyright (c) 2021 - for information on the respective copyright owner
 * see the NOTICE file and/or the repository https://github.com/carbynestack/castor.
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package io.carbynestack.castor.service.persistence.markerstore;

import io.carbynestack.castor.common.entities.ActivationStatus;
import io.carbynestack.castor.common.entities.TupleChunk;
import io.carbynestack.castor.common.entities.TupleType;
import java.io.Serializable;
import java.util.UUID;
import javax.persistence.*;
import lombok.AccessLevel;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.experimental.Accessors;
import org.springframework.util.Assert;

/**
 * An instance of this objects provides information about size and already reserved/consumed tuple
 * shares in a {@link TupleChunk} object. Since tuple Shares are not stored separately {@link
 * TupleChunk} in a chunk it is possible to identify position (in Bytes) of a tuple Share by using
 * {@link TupleType#getTupleSize()}.
 */
@Data
@Accessors(chain = true)
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
@Entity
@Table(name = TupleChunkMetaDataEntity.TABLE_NAME)
public class TupleChunkMetaDataEntity implements Serializable {
  private static final long serialVersionUID = -2835836059298562970L;

  @Transient public static final String TABLE_NAME = "tupleChunkMetaData";

  @Transient public static final String TUPLE_CHUNK_ID_COLUMN = "tuple_chunk_id";

  @Transient public static final String NUMBER_OF_TUPLES_COLUMN = "number_of_tuples";

  @Transient public static final String RESERVED_MARKER_COLUMN = "reserved_marker";

  @Transient public static final String CONSUMED_MARKER_COLUMN = "consumed_marker";

  @Transient public static final String TUPLE_TYPE_COLUMN = "tuple_type";

  @Transient public static final String STATUS_COLUMN = "status";

  public static final String CLASS_NAME = "TupleChunkMetaDataEntity";
  public static final String NUMBER_OF_TUPLES_FIELD = "numberOfTuples";
  public static final String TUPLE_TYPE_FIELD = "tupleType";
  public static final String STATUS_FIELD = "status";

  public static final String ID_MUST_NOT_BE_NULL_EXCEPTION_MSG = "TupleChunk ID must not be null.";
  public static final String TUPLE_TYPE_MUST_NOT_BE_NULL_EXCEPTION_MSG =
      "TupleType must not be null.";
  public static final String INVALID_NUMBER_OF_TUPLES_EXCEPTION_MSG =
      "At least 1 tuple share is required to store meta data of a tuple chunk.";
  public static final String MARKER_OUT_OF_RANGE_EXCEPTION_MSG = "Given marker out of range.";

  /**
   * Unique identifier of the {@link TupleChunk} referenced by this {@link
   * TupleChunkMetaDataEntity}.
   */
  @Id
  @Column(name = TUPLE_CHUNK_ID_COLUMN)
  private final UUID tupleChunkId;

  /**
   * Number of tuple shares stored in the {@link TupleChunk} referenced by this {@link
   * TupleChunkMetaDataEntity}, starting at 0.
   */
  @Column(name = NUMBER_OF_TUPLES_COLUMN)
  private final long numberOfTuples;

  /**
   * Marks the count of already consumed tuple shares in the {@link TupleChunk} referenced by this
   * {@link TupleChunkMetaDataEntity}, starting at 0.
   *
   * <p>Setting the {@link #reservedMarker} equal to {@link #numberOfTuples} indicates all tuples in
   * the referenced {@link TupleChunk} to be reserved.
   */
  @Column(name = RESERVED_MARKER_COLUMN)
  private long reservedMarker = 0;

  /**
   * Marks the count of already consumed tuple shares in the {@link TupleChunk} referenced by this
   * {@link TupleChunkMetaDataEntity}, starting at 0.
   *
   * <p>Setting {@link #consumedMarker} equal to {@link #numberOfTuples} indicates all tuples in the
   * referenced {@link TupleChunk} to be consumed.
   */
  @Column(name = CONSUMED_MARKER_COLUMN)
  private long consumedMarker = 0;

  /**
   * The {@link TupleType} of the tuples stored in the @link TupleChunk} referenced by this * {@link
   * TupleChunkMetaDataEntity}
   */
  @Column(name = TUPLE_TYPE_COLUMN)
  private final TupleType tupleType;

  /**
   * Indicator whether the referenced {@link TupleChunk} is cleared for consumption. (see {@link
   * ActivationStatus})
   */
  @Column(name = STATUS_COLUMN)
  @Enumerated(EnumType.STRING)
  private ActivationStatus status = ActivationStatus.LOCKED;

  /** @deprecated to be used by deserialization only */
  @Deprecated
  protected TupleChunkMetaDataEntity() {
    this.tupleChunkId = null;
    this.tupleType = null;
    this.numberOfTuples = 0;
  }

  /**
   * Set count of already reserved tuple shares in the {@link TupleChunk} referenced by this {@link
   * TupleChunkMetaDataEntity}, starting at position 0.
   *
   * <p>Setting the {@link #reservedMarker} equal to {@link #numberOfTuples} indicates all tuples in
   * the referenced {@link TupleChunk} to be reserved.
   *
   * @param reservedMarker The amount of reserved tuple shares in chunk identified by {@link
   *     #tupleChunkId}. starting at position 0.
   * @return This instance of TupleChunkMetaDataEntity.
   * @throws IllegalArgumentException if reservedMarker is not within range of tuples stored in the
   *     related tuple
   */
  public TupleChunkMetaDataEntity setReservedMarker(long reservedMarker) {
    if (reservedMarker < 0 || reservedMarker > this.numberOfTuples) {
      throw new IllegalArgumentException(MARKER_OUT_OF_RANGE_EXCEPTION_MSG);
    }
    this.reservedMarker = reservedMarker;
    return this;
  }

  /**
   * Set count of already consumed tuple shares in the {@link TupleChunk} referenced by this {@link
   * TupleChunkMetaDataEntity}, starting at position 0.
   *
   * <p>Setting the {@link #consumedMarker} equal to {@link #numberOfTuples} indicates all tuples in
   * the referenced {@link TupleChunk} to be consumed.
   *
   * @param consumedMarker The amount of consumed tuple shares in chunk identified by {@link
   *     #tupleChunkId}. starting at position 0.
   * @return This instance of {@link TupleChunkMetaDataEntity}.
   * @throws IllegalArgumentException if consumptionMarker is not within range of number of tuples
   *     stored in the related tuple chunk
   */
  public TupleChunkMetaDataEntity setConsumedMarker(long consumedMarker) {
    if (consumedMarker < 0 || consumedMarker > this.numberOfTuples) {
      throw new IllegalArgumentException(MARKER_OUT_OF_RANGE_EXCEPTION_MSG);
    }
    this.consumedMarker = consumedMarker;
    return this;
  }

  /**
   * Creates a new {@link TupleChunkMetaDataEntity} with the given configuration and default {@link
   * #getStatus()} {@link ActivationStatus#LOCKED} as well as {@link #getReservedMarker()} and
   * {@link #getConsumedMarker()} set to 0.
   *
   * @param tupleChunkId Unique identifier of the {@link TupleChunk} referenced by this {@link
   *     TupleChunkMetaDataEntity}.
   * @param tupleType The {@link TupleType} of the tuples stored in the @link TupleChunk} referenced
   *     by this {@link TupleChunkMetaDataEntity}.
   * @param numberOfTuples Number of stored tuple shares in chunk identified by {@link
   *     #tupleChunkId}.
   * @return
   */
  public static TupleChunkMetaDataEntity of(
      UUID tupleChunkId, TupleType tupleType, long numberOfTuples) {
    Assert.notNull(tupleChunkId, ID_MUST_NOT_BE_NULL_EXCEPTION_MSG);
    Assert.notNull(tupleType, TUPLE_TYPE_MUST_NOT_BE_NULL_EXCEPTION_MSG);
    if (numberOfTuples < 1) {
      throw new IllegalArgumentException(INVALID_NUMBER_OF_TUPLES_EXCEPTION_MSG);
    }
    return new TupleChunkMetaDataEntity(tupleChunkId, numberOfTuples, tupleType);
  }
}
