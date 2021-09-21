/*
 * Copyright (c) 2021 - for information on the respective copyright owner
 * see the NOTICE file and/or the repository https://github.com/carbynestack/castor.
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package io.carbynestack.castor.common.entities;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import java.io.IOException;

public class TupleListSerializer<T extends Tuple<T, F>, F extends Field>
    extends StdSerializer<TupleList<T, F>> {
  private static final long serialVersionUID = 6014009842273046357L;

  public TupleListSerializer() {
    this(null);
  }

  public TupleListSerializer(Class<TupleList<T, F>> t) {
    super(t);
  }

  @Override
  public void serialize(TupleList<T, F> tupleList, JsonGenerator jgen, SerializerProvider provider)
      throws IOException {
    jgen.writeStartObject();
    jgen.writeObjectField("tupleCls", tupleList.getTupleCls().getSimpleName());
    jgen.writeObjectField("field", tupleList.getField());
    jgen.writeArrayFieldStart("tuples");
    for (Tuple tuple : tupleList) {
      jgen.writeObject(tuple);
    }
    jgen.writeEndArray();
    jgen.writeEndObject();
  }
}
