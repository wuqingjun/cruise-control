/*
 * Copyright 2026 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.acl;

/**
 * The ACL file can't be used: unreadable, not valid JSON, wrong structure, or invalid content. When this is thrown,
 * nothing is applied and the ACLs in ZooKeeper are left unchanged.
 */
public class AclFileException extends Exception {
  private static final long serialVersionUID = 1L;

  /**
   * @param message What is wrong, including the location in the file (e.g. {@code kafka-acl/Topic/orders}).
   */
  public AclFileException(String message) {
    super(message);
  }

  /**
   * @param message What is wrong, including the location in the file.
   * @param cause The underlying error.
   */
  public AclFileException(String message, Throwable cause) {
    super(message, cause);
  }
}
