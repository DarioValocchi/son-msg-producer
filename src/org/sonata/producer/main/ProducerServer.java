package org.sonata.producer.main;

import java.io.File;
import java.util.HashMap;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

import org.sonata.producer.rabbit.RabbitMqConsumer;
import org.sonata.producer.rabbit.RabbitMqProducer;
import org.sonata.producer.rabbit.ServicePlatformMessage;

import net.sourceforge.argparse4j.inf.Namespace;

public class ProducerServer implements Runnable {

  private ExecutorService myThreadPool;
  private Namespace request;
  private HashMap<String, Generator> threadMap;
  private RabbitMqProducer producer;
  private RabbitMqConsumer consumer;
  private boolean stop = false;
  private BlockingQueue<ServicePlatformMessage> muxQueue;
  private BlockingQueue<ServicePlatformMessage> myQueue;
  private static int requestSent=0;
  private static int requestFailed=0;
  
  public ProducerServer(Namespace request){
    this.request=request;
    this.myThreadPool = Executors.newFixedThreadPool(request.getInt("threads"));
    this.threadMap = new HashMap<String,Generator>();
    muxQueue = new LinkedBlockingQueue<ServicePlatformMessage>(request.getInt("threads")+1);
    this.myQueue= new LinkedBlockingQueue<ServicePlatformMessage>(request.getInt("threads")+1);
    this.producer = new RabbitMqProducer(muxQueue, request);
    this.consumer = new RabbitMqConsumer(myQueue, request);
  }

  public void start() {
    System.out.println("Starting server with parameters:");
    System.out.println(request);
    producer.connectToBus();
    consumer.connectToBus();
    producer.startProducing();
    consumer.startConsuming();
    new Thread(this).start();
    
    for (int i = 0; i < request.getInt("threads"); i++) {
      Generator gen =
          new Generator(request.getInt("requests"), (File) request.get("folder"), threadMap, muxQueue);
      myThreadPool.execute(gen);
    }
    myThreadPool.shutdown();
    
    //TODO write stats;
    System.out.println("Test Finished.\nRequest sent= "+ProducerServer.requestSent+"\nRequestFailed="+ProducerServer.requestFailed);
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
        th.update(null, message);
        
      } catch (InterruptedException e) {
        System.out.println(e.getMessage());
      }
    } while (!stop);
  }

  private void stop() {
    this.stop = true;
  }

  public static synchronized void incRequestsFailed(){
    requestFailed++;
  }
  public static synchronized void incRequestsSent(){
    requestSent++;
  }
}
