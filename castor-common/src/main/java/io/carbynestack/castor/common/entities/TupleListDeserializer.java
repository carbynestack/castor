/*
 * Copyright (c) 2021 - for information on the respective copyright owner
 * see the NOTICE file and/or the repository https://github.com/carbynestack/castor.
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package io.carbynestack.castor.common.entities;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.node.ArrayNode;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class TupleListDeserializer extends StdDeserializer<TupleList> {
  private static final long serialVersionUID = -8874805485630331926L;

  public TupleListDeserializer() {
    this(null);
  }

  public TupleListDeserializer(Class<?> vc) {
    super(vc);
  }

  @Override
  public TupleList deserialize(JsonParser jp, DeserializationContext ctxt) throws IOException {
    ObjectMapper objectMapper = new ObjectMapper();
    JsonNode node = jp.getCodec().readTree(jp);
    try {
      Class<? extends Tuple> tupleCls =
          (Class<? extends Tuple>)
              Class.forName(
                  String.format(
                      "%s.%s",
                      Tuple.class.getPackage().getName(), node.get("tupleCls").textValue()));
      Field field = objectMapper.readValue(node.get("field").toString(), Field.class);
      List<Tuple> tuples = new ArrayList<>();
      for (JsonNode tupleNode : ((ArrayNode) node.get("tuples"))) {
        tuples.add(objectMapper.readValue(tupleNode.toString(), Tuple.class));
      }
      return new TupleList(tupleCls, field, tuples);
    } catch (ClassNotFoundException e) {
      throw new JsonParseException(jp, e.getMessage());
    }
  }
}
