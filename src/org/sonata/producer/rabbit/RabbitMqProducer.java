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

import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import net.sourceforge.argparse4j.inf.Namespace;

import org.slf4j.LoggerFactory;


import java.io.IOException;
import java.net.URISyntaxException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeoutException;

public class RabbitMqProducer extends AbstractMsgBusProducer {

  
  
  private static final org.slf4j.Logger Logger = LoggerFactory.getLogger(RabbitMqProducer.class);
  private Namespace request;
  private Connection connection;

  public RabbitMqProducer(BlockingQueue<ServicePlatformMessage> muxQueue, Namespace request) {
    super(muxQueue);
    this.request=request;

  }

  @Override
  public void connectToBus() {
    
    ConnectionFactory cf = new ConnectionFactory();
    
    try {
      String uri= request.getString("url");
      Logger.info("Connecting producer to: " + request.getString("url"));
      cf.setUri(uri);
    } catch (KeyManagementException e) {
      Logger.error(e.getMessage(), e);
    } catch (NoSuchAlgorithmException e) {
      Logger.error(e.getMessage(), e);
    } catch (URISyntaxException e) {
      Logger.error(e.getMessage(), e);
    }

    try {
      connection = cf.newConnection();
    } catch (IOException e) {
      Logger.error(e.getMessage(), e);
    } catch (TimeoutException e) {
      Logger.error(e.getMessage(), e);
    }
  }

  @Override
  public boolean sendMessage(ServicePlatformMessage message) {
    boolean out = true;

    // TODO maps the specific Adaptor message to the proper SP topic

    try {
      Channel channel = connection.createChannel();
      String exchangeName = request.getString("exchange");
      channel.exchangeDeclare(exchangeName, "topic");
      BasicProperties properties = new BasicProperties().builder().appId("org.sonata.TestProducer")
          .contentType(message.getContentType()).replyTo(message.getReplyTo())
          .correlationId(message.getSid()).build();
      channel.basicPublish(exchangeName, message.getTopic(), properties,
          message.getBody().getBytes("UTF-8"));
      // Logger.info("Sending message: " + message + "\n\r - Properties:" + properties);
    } catch (Exception e) {
      Logger.error(e.getMessage(), e);
      out = false;
    }
    return out;
  }

}
