package org.sonata.producer.main;

import java.io.File;
import java.util.HashMap;
import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.slf4j.LoggerFactory;
import org.sonata.producer.rabbit.RabbitMqConsumer;
import org.sonata.producer.rabbit.RabbitMqHelperSingleton;
import org.sonata.producer.rabbit.RabbitMqProducer;
import org.sonata.producer.rabbit.ServicePlatformMessage;

import net.sourceforge.argparse4j.inf.Namespace;

public class ProducerServer implements Runnable {

  private static final org.slf4j.Logger Logger = LoggerFactory.getLogger(ProducerServer.class);

  private ExecutorService myThreadPool;
  private Namespace request;
  private ConcurrentHashMap<String, Generator> threadMap;
  private RabbitMqProducer producer;
  private RabbitMqConsumer consumer;
  private boolean stop = false;
  private BlockingQueue<ServicePlatformMessage> muxQueue;
  private BlockingQueue<ServicePlatformMessage> myQueue;
  private static int requestSent = 0;
  private static int requestFailed = 0;

  public ProducerServer(Namespace request) {
    this.request = request;
    this.myThreadPool = Executors.newCachedThreadPool();
    this.threadMap = new ConcurrentHashMap<String, Generator>();
    muxQueue = new LinkedBlockingQueue<ServicePlatformMessage>(request.getInt("threads") + 1);
    this.myQueue = new LinkedBlockingQueue<ServicePlatformMessage>(request.getInt("threads") + 1);
    RabbitMqHelperSingleton.setRequestProp(request);
    this.producer = new RabbitMqProducer(muxQueue);
    this.consumer = new RabbitMqConsumer(myQueue);
  }

  public void start() {
    Logger.debug("Starting server with parameters:");
    Logger.debug(request.toString());
    Random ran = new Random(System.currentTimeMillis());
    producer.connectToBus();
    consumer.connectToBus();

    producer.startProducing();
    consumer.startConsuming();
    new Thread(this).start();

    try {
      Thread.sleep(5000);
    } catch (InterruptedException e1) {
      // TODO Auto-generated catch block
      e1.printStackTrace();
    }
    for (int i = 0; i < request.getInt("threads"); i++) {
      Generator gen = new Generator(request.getInt("requests"), (File) request.get("folder"),
          threadMap, muxQueue, i + 1, ran);
      myThreadPool.execute(gen);
//      try {
//        Thread.sleep(100);
//      } catch (InterruptedException e) {
//        // TODO Auto-generated catch block
//        e.printStackTrace();
//      }
    }
    myThreadPool.shutdown();
    try {
      myThreadPool.awaitTermination(24, TimeUnit.HOURS);
    } catch (InterruptedException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    // TODO write stats;
    Logger.info("Test Finished.");
    Logger.info("Request sent = " + ProducerServer.requestSent);
    Logger.info("RequestFailed = " + ProducerServer.requestFailed);
    stop();
    producer.stopProducing();
    consumer.stopConsuming();
    return;
  }

  @Override
  public void run() {
    ServicePlatformMessage message;
    do {
      try {
        message = myQueue.take();
        String sid = message.getSid();
        Generator th = threadMap.get(sid);
        Logger.debug("Response received for Producer-" +th.getThreadIndex());
        th.update(null, message);

      } catch (InterruptedException e) {
        System.out.println(e.getMessage());
      }
    } while (!stop);
  }

  private void stop() {
    this.stop = true;
  }

  public static synchronized void incRequestsFailed() {
    requestFailed++;
  }

  public static synchronized void incRequestsSent() {
    requestSent++;
  }
}
