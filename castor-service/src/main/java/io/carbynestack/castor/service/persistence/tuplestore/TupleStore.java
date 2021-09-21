/*
 * Copyright (c) 2021 - for information on the respective copyright owner
 * see the NOTICE file and/or the repository https://github.com/carbynestack/castor.
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package io.carbynestack.castor.service.persistence.tuplestore;

import io.carbynestack.castor.common.entities.Field;
import io.carbynestack.castor.common.entities.Tuple;
import io.carbynestack.castor.common.entities.TupleChunk;
import io.carbynestack.castor.common.entities.TupleList;
import io.carbynestack.castor.common.exceptions.CastorServiceException;
import java.util.UUID;

public interface TupleStore {

  /**
   * Stores a {@link TupleChunk} in the data store.
   *
   * <p>This method will use the chunks {@link TupleChunk#getChunkId()} as unique identifier of the
   * chunk.
   *
   * @param tupleChunk the {@link TupleChunk} to persist.
   * @throws CastorServiceException if storing fails
   */
  void save(TupleChunk tupleChunk);

  /**
   * Downloads multiplication triples from a chunk in the store.
   *
   * @param tupleCls Class of tuples that sould be downloaded
   * @param fieldType The filedType of the requested tuples
   * @param tupleChunkId Identifies, from which chunk to download
   * @param startIndex the index of the first byte to read (inclusive)
   * @param lengthToRead the number of bytes to read from the referenced chunk
   * @return all multiplication triples in the specified range
   * @throws CastorServiceException if some error occurs while reading data stream from store
   * @throws IllegalArgumentException if given index of first requested tuple is negative.
   * @throws IllegalArgumentException if less than one tuple is requested.
   */
  <T extends Tuple<T, F>, F extends Field> TupleList<T, F> downloadTuples(
      Class<T> tupleCls, F fieldType, UUID tupleChunkId, long startIndex, long lengthToRead);

  /**
   * Deletes an object from the store
   *
   * @param id the id of the object to delete
   */
  void deleteTupleChunk(UUID id);
}
