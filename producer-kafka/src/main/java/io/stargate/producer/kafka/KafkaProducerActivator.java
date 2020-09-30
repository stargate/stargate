/*
 * Copyright 2018-2020 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.stargate.producer.kafka;

import io.stargate.db.cdc.CDCProducer;
import org.osgi.framework.BundleActivator;
import org.osgi.framework.BundleContext;
import org.osgi.framework.ServiceEvent;
import org.osgi.framework.ServiceListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/* Logic for registering the kafka producer as an OSGI bundle */
public class KafkaProducerActivator implements BundleActivator, ServiceListener {
  private static final Logger logger = LoggerFactory.getLogger(KafkaProducerActivator.class);

  @Override
  public synchronized void start(BundleContext context) {
    logger.info("Registering Kafka producer...");
    // TODO: Set mapping service and schema provider
    CDCProducer producer = new KafkaCDCProducer(null, null);
    context.registerService(CDCProducer.class, producer, null);
  }

  @Override
  public synchronized void stop(BundleContext context) {}

  @Override
  public synchronized void serviceChanged(ServiceEvent event) {}
}
