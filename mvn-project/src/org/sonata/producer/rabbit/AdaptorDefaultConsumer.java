/*
 * Copyright (c) 2015 SONATA-NFV, UCL, NOKIA, NCSR Demokritos ALL RIGHTS RESERVED.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 * 
 * Neither the name of the SONATA-NFV, UCL, NOKIA, NCSR Demokritos nor the names of its contributors
 * may be used to endorse or promote products derived from this software without specific prior
 * written permission.
 * 
 * This work has been performed in the framework of the SONATA project, funded by the European
 * Commission under Grant number 671517 through the Horizon 2020 and 5G-PPP programmes. The authors
 * would like to acknowledge the contributions of their colleagues of the SONATA partner consortium
 * (www.sonata-nfv.eu).
 *
 * @author Dario Valocchi (Ph.D.), UCL
 * 
 */

package org.sonata.producer.rabbit;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;

import org.slf4j.LoggerFactory;


import java.io.IOException;


public class AdaptorDefaultConsumer extends DefaultConsumer {

  private static final org.slf4j.Logger Logger =
      LoggerFactory.getLogger(AdaptorDefaultConsumer.class);

  private RabbitMqConsumer msgBusConsumer;

  /**
   * Create a RabbitMq consumer for the MsgBus plug-in.
   * 
   * @param channel the RabbitMQ channel for this consumer
   * @param msgBusConsumer the Adaptor consumer, responsible for msg processing and queuing.
   */
  public AdaptorDefaultConsumer(Channel channel, RabbitMqConsumer msgBusConsumer) {
    super(channel);
    this.msgBusConsumer = msgBusConsumer;
  }

  @Override
  public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties,
      byte[] body) throws IOException {
    String message = new String(body, "UTF-8");
    //Logger.debug("Received message:" + message + " on " + envelope.getRoutingKey());
    //Logger.debug("Received message on " + envelope.getRoutingKey());
    if (properties != null && properties.getAppId() != null
        && properties.getAppId().equals("sonata.kernel.WimAdapter")
        && envelope.getRoutingKey().equals("infrastructure.service.deploy")) {
      //Logger.debug("Ignoring WIM adaptor response after service deployment");
    } else if (properties != null && properties.getAppId() != null
        && !properties.getAppId().equals("org.sonata.TestProducer")) {
      this.msgBusConsumer.processMessage(message, properties.getContentType(),
          envelope.getRoutingKey(), properties.getCorrelationId(), properties.getReplyTo());
    } else {
      //Logger.debug("Message ignored: " + properties);
    }
  }

}
