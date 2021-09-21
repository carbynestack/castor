/*
 * Copyright (c) 2021 - for information on the respective copyright owner
 * see the NOTICE file and/or the repository https://github.com/carbynestack/castor.
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package io.carbynestack.castor.common.entities;

/**
 * Enumeration to indicate the status of tuple chunks and reservations.
 *
 * <p>This status is used internally to ensure that the stored tuple chunk and reservation can
 * actually be consumed. <br>
 * When stored in the database, both types are initially flagged as {@link #LOCKED}. Only if each
 * party in the Carbyne Stack MPC Cluster has received its share of the information, the given entry
 * should be activated for consumption and therefore be marked as {@link #UNLOCKED}.
 */
public enum ActivationStatus {
  /**
   * Indicates that a given resource is not cleared for consumption, as it might not have been
   * successfully shared with all parties of the cluster.
   */
  LOCKED,
  /** Indicates that the given resource is cleared for consumption. */
  UNLOCKED;
}
