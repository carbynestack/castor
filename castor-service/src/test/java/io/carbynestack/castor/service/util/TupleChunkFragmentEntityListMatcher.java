/*
 * Copyright (c) 2022 - for information on the respective copyright owner
 * see the NOTICE file and/or the repository https://github.com/carbynestack/castor.
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package io.carbynestack.castor.service.util;

import com.google.common.base.Objects;
import io.carbynestack.castor.service.persistence.fragmentstore.TupleChunkFragmentEntity;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;

/**
 * Matcher to verify that a list of {@link TupleChunkFragmentEntity TupleChunkFragmentEntities}
 * contains an item with the given attributes, while the {@link TupleChunkFragmentEntity}'s id is
 * ignored.
 */
@RequiredArgsConstructor
public class TupleChunkFragmentEntityListMatcher
    extends TypeSafeMatcher<List<TupleChunkFragmentEntity>> {
  private final List<TupleChunkFragmentEntity> expectedEntities;
  private String failureMsg = "";

  @Override
  protected boolean matchesSafely(List<TupleChunkFragmentEntity> actual) {
    for (TupleChunkFragmentEntity expected : expectedEntities) {
      if (actual.stream()
          .noneMatch(
              e ->
                  e.getTupleChunkId().equals(expected.getTupleChunkId())
                      && e.getTupleType().equals(expected.getTupleType())
                      && e.getStartIndex() == expected.getStartIndex()
                      && e.getEndIndex() == expected.getEndIndex()
                      && Objects.equal(e.getActivationStatus(), expected.getActivationStatus())
                      && Objects.equal(e.getReservationId(), expected.getReservationId()))) {
        failureMsg =
            String.format("{%s} not found in: [\n", getFragmentEntityDescription(expected));
        failureMsg +=
            actual.stream()
                .map(e -> "\t\t{" + getFragmentEntityDescription(e) + "}")
                .collect(Collectors.joining(",\n"));
        failureMsg += "\n\t]";
        return false;
      }
    }
    return true;
  }

  @Override
  public void describeTo(Description description) {
    description.appendText("a list of TupleChunkFragmentEntities containing:");
    for (TupleChunkFragmentEntity entity : expectedEntities) {
      description.appendText(String.format("%n\t{%s}", getFragmentEntityDescription(entity)));
    }
  }

  @Override
  public void describeMismatchSafely(
      List<TupleChunkFragmentEntity> tags, Description mismatchDescription) {
    mismatchDescription.appendText(failureMsg);
  }

  public static Matcher<List<TupleChunkFragmentEntity>> containsAll(
      TupleChunkFragmentEntity... fragmentEntity) {
    return new TupleChunkFragmentEntityListMatcher(Arrays.asList(fragmentEntity));
  }

  private String getFragmentEntityDescription(TupleChunkFragmentEntity entity) {
    return String.format(
        "tupleChunkId: %s, "
            + "tupleType: %s, "
            + "startIndex: %d, "
            + "endIndex: %d, "
            + "activationStatus: %s, "
            + "reservationId: %s",
        entity.getTupleChunkId(),
        entity.getTupleType(),
        entity.getStartIndex(),
        entity.getEndIndex(),
        entity.getActivationStatus(),
        entity.getReservationId());
  }
}
