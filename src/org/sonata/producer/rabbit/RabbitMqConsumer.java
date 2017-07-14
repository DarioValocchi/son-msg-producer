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

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DefaultConsumer;

import net.sourceforge.argparse4j.inf.Namespace;

import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeoutException;

public class RabbitMqConsumer extends AbstractMsgBusConsumer implements MsgBusConsumer, Runnable {

  private static final org.slf4j.Logger Logger = LoggerFactory.getLogger(RabbitMqConsumer.class);
  private Channel channel;
  private Connection connection;
  private String queueName;
  private Namespace request;

  DefaultConsumer consumer;

  public RabbitMqConsumer(BlockingQueue<ServicePlatformMessage> dispatcherQueue, Namespace request) {
    super(dispatcherQueue);
    this.request=request;
  }

  @Override
  public void connectToBus() {
    try {
      ConnectionFactory cf = new ConnectionFactory();
      
      Logger.info("Connecting consumer to: " + request.getString("url"));
      cf.setUri(request.getString("url"));
      connection = cf.newConnection();
      channel = connection.createChannel();
      String exchangeName = request.getString("exchange");
      channel.exchangeDeclare(exchangeName, "topic");
      queueName = exchangeName + "." + "InfraAbstract";
      channel.queueDeclare(queueName, true, false, false, null);
      Logger.info("Binding queue to topics...");

      channel.queueBind(queueName, exchangeName, "platform.management.plugin.register");
      Logger.info("Bound to topic \"platform.platform.management.plugin.register\"");

      channel.queueBind(queueName, exchangeName, "platform.management.plugin.deregister");
      Logger.info("Bound to topic \"platform.platform.management.plugin.deregister\"");

      channel.queueBind(queueName, exchangeName, "infrastructure.#");
      Logger.info("[northbound] RabbitMqConsumer - bound to topic \"infrastructure.#\"");

      consumer = new AdaptorDefaultConsumer(channel, this);
    } catch (TimeoutException e) {
      Logger.error(e.getMessage(), e);
    } catch (KeyManagementException e) {
      Logger.error(e.getMessage(), e);
    } catch (NoSuchAlgorithmException e) {
      Logger.error(e.getMessage(), e);
    } catch (URISyntaxException e) {
      Logger.error(e.getMessage(), e);
    } catch (IOException e) {
      Logger.error(e.getMessage(), e);
    }

  }

  @Override
  public void run() {
    try {
      Logger.info("Starting consumer thread");
      channel.basicConsume(queueName, true, consumer);
    } catch (IOException e) {
      Logger.error(e.getMessage(), e);
    }
  }

  @Override
  public boolean startConsuming() {
    boolean out = true;
    Thread thread;
    try {
      thread = new Thread(this);
      thread.start();
    } catch (Exception e) {
      Logger.error(e.getMessage(), e);
      out = false;
    }
    return out;
  }

  @Override
  public boolean stopConsuming() {
    boolean out = true;
    try {
      channel.close();
      connection.close();
    } catch (IOException e) {
      Logger.error(e.getMessage(), e);
      out = false;
    } catch (TimeoutException e) {
      Logger.error(e.getMessage(), e);
      out = false;
    }

    return out;
  }

}
