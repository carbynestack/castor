/*
 * Copyright (c) 2021 - for information on the respective copyright owner
 * see the NOTICE file and/or the repository https://github.com/carbynestack/castor.
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package io.carbynestack.castor.common.entities;

import static java.util.Collections.emptyList;
import static org.apache.commons.lang3.exception.ExceptionUtils.rethrow;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import lombok.Value;

/** A list of {@link Tuple}s. Used for downloading multiple tuples in one request. */
@Value
@EqualsAndHashCode(callSuper = true)
@ToString(callSuper = true)
@JsonSerialize(using = TupleListSerializer.class)
@JsonDeserialize(using = TupleListDeserializer.class)
public class TupleList<T extends Tuple<T, F>, F extends Field> extends ArrayList<T>
    implements Serializable {
  private static final long serialVersionUID = 5616885721125435294L;

  F field;
  Class<T> tupleCls;

  TupleList(Class<T> tupleCls, F field, List<T> tuples) {
    super(tuples);
    this.field = field;
    this.tupleCls = tupleCls;
  }

  public TupleList(Class<T> tupleCls, F field) {
    this(tupleCls, field, emptyList());
  }

  /**
   * This method allows reading an InputStream and add content as tuple instances to this TupleList.
   *
   * @param inputStream InputStream containing tuples as a sequence of shares
   * @param length Number of bytes assumed to be read
   * @throws IOException in case of errors reading InputStream
   */
  public static <T extends Tuple<T, F>, F extends Field> TupleList<T, F> fromStream(
      Class<T> tupleCls, F field, InputStream inputStream, long length) throws IOException {
    TupleType tupleType = TupleType.findTupleType(tupleCls, field);
    try {
      Constructor<T> constructor = tupleCls.getDeclaredConstructor(Field.class, InputStream.class);
      List<T> tuples = new ArrayList<>();

      for (int i = 0; i < length / tupleType.getTupleSize(); i++) {
        tuples.add(constructor.newInstance(field, inputStream));
      }
      return new TupleList<>(tupleCls, field, tuples);
    } catch (Exception e) {
      throw new IOException("Failed reading tuples from stream.", e);
    }
  }

  public byte[] toByteArray(){
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    this.forEach(
            tuple -> {
              try {
                tuple.writeTo(outputStream);
              } catch (IOException exception) {
                rethrow(exception);
              }
            });
    return outputStream.toByteArray();
  }

  /**
   * Converts a {@link TupleList} to {@link TupleChunk}
   *
   * @param uuid The globally unique identifier for the chunk
   * @return A {@link TupleChunk} containing the tuples from this {@link TupleList}
   * @throws IOException in case of I/O errors
   */
  public final TupleChunk<T, F> asChunk(UUID uuid) throws IOException {
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    this.forEach(
        tuple -> {
          try {
            tuple.writeTo(outputStream);
          } catch (IOException exception) {
            rethrow(exception);
          }
        });
    return TupleChunk.of(tupleCls, field, uuid, outputStream.toByteArray());
  }
}
