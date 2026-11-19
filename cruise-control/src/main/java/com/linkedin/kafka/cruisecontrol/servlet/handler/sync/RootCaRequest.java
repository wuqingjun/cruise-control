/*
 * Copyright 2026 qjwu. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.handler.sync;

import com.linkedin.kafka.cruisecontrol.KafkaCruiseControl;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.servlet.parameters.RootCaParameters;
import com.linkedin.kafka.cruisecontrol.servlet.response.RootCaState;
import java.util.Map;

import static com.linkedin.cruisecontrol.common.utils.Utils.validateNotNull;
import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.ROOT_CA_PARAMETER_OBJECT_CONFIG;

/**
 * Handles the <code>root_ca</code> endpoint: the root CA thumbprints clients should pin, checked against the certificate
 * chains the brokers send.
 */
public class RootCaRequest extends AbstractSyncRequest {
  protected KafkaCruiseControl _kafkaCruiseControl;
  protected KafkaCruiseControlConfig _config;
  protected RootCaParameters _parameters;

  public RootCaRequest() {
    super();
  }

  @Override
  protected RootCaState handle() {
    return new RootCaState(_config, _kafkaCruiseControl.rootCaMonitor().report());
  }

  @Override
  public RootCaParameters parameters() {
    return _parameters;
  }

  @Override
  public String name() {
    return RootCaRequest.class.getSimpleName();
  }

  @Override
  public void configure(Map<String, ?> configs) {
    super.configure(configs);
    _kafkaCruiseControl = getCruiseControlEndpoints().asyncKafkaCruiseControl();
    _config = _kafkaCruiseControl.config();
    _parameters = (RootCaParameters) validateNotNull(configs.get(ROOT_CA_PARAMETER_OBJECT_CONFIG),
                                                     "Parameter configuration is missing from the request.");
  }
}
