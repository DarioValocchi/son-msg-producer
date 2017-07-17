package org.sonata.producer.main;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.HashMap;
import java.util.Map;
import java.util.Observable;
import java.util.Observer;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;

import javax.sql.rowset.spi.SyncFactory;

import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONTokener;
import org.slf4j.LoggerFactory;
import org.sonata.producer.rabbit.ServicePlatformMessage;

import com.esotericsoftware.yamlbeans.YamlException;
import com.esotericsoftware.yamlbeans.YamlReader;

public class Generator implements Runnable, Observer {

  private static final org.slf4j.Logger Logger = LoggerFactory.getLogger(Generator.class);

  private ConcurrentHashMap<String, Generator> lockMap;
  private int maxRequests;
  private File requestsFolder;
  private BlockingQueue<ServicePlatformMessage> muxQueue;
  private Random rand;
  private final Object lock;
  private String currentUuid = null;
  private File currentRequestFile = null;
  private int threadIndex;

  public Generator(int r, File f, ConcurrentHashMap<String, Generator> m,
      BlockingQueue<ServicePlatformMessage> muxQueue, int index, Random rand) {
    this.requestsFolder = f;
    this.maxRequests = r;
    this.lockMap = m;
    this.muxQueue = muxQueue;
    this.lock = new Object();
    this.threadIndex = index;
    this.rand=rand;
  }

  @Override
  public void run() {
    int requestDone = 0;
    while (requestDone < this.maxRequests) {
      currentUuid = UUID.randomUUID().toString();
      lockMap.put(currentUuid, this);
      sendRequest(currentUuid);
      try {
        synchronized (lock) {
          lock.wait();
        }
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
      lockMap.remove(currentUuid);
      currentUuid = null;
      currentRequestFile = null;
      requestDone++;
    }

  }

  @Override
  public void update(Observable o, Object arg) {
    ServicePlatformMessage response = (ServicePlatformMessage) arg;
    // TODO log the response result;
    JSONTokener tokener;
    try {
      tokener = new JSONTokener(new FileInputStream(currentRequestFile));
    } catch (JSONException e) {
      e.printStackTrace();
      return;
    } catch (FileNotFoundException e) {
      e.printStackTrace();
      return;
    }
    JSONObject object = ((JSONObject) tokener.nextValue()).getJSONObject("response");

    String expKey = object.getString("key_to_read");
    String expValue = object.getString("expected_value");
    String responseBody= response.getBody().replace("\n", "");
    Logger.debug("received response:" + responseBody);

    if (response.getContentType().equals("application/json")) {
      tokener = new JSONTokener(responseBody);
      object = (JSONObject) tokener.nextValue();
      String result = object.getString(expKey);
      if (!object.has(expKey) || !result.equals(expValue)) {
        ProducerServer.incRequestsFailed();
      }else{
        Logger.info("Request successfully completed.");
      }
    } else if (response.getContentType().equals("application/xyaml")) {
      YamlReader reader = new YamlReader(responseBody);
      Object obj;
      try {
        obj = reader.read();
        Map<String, Object> map = (Map<String, Object>) obj;
        if (!map.containsKey(expKey) || !map.get(expKey).equals(expValue)) {
          ProducerServer.incRequestsFailed();
        } else{
          Logger.info("Request successfully completed.");
        }
      } catch (YamlException e) {
        e.printStackTrace();
      }
    }
    synchronized (lock) {
      lock.notify(); 
    }
  }

  private void sendRequest(String uuid) {
    File[] fileList = this.requestsFolder.listFiles();
    int requestNumber = rand.nextInt(fileList.length);
    currentRequestFile = fileList[requestNumber];
    Logger.info("Producer-" + this.threadIndex + " - sending " + currentRequestFile.getName()+" - SID: "+uuid);
    JSONTokener tokener;
    try {
      tokener = new JSONTokener(new FileInputStream(currentRequestFile));
    } catch (JSONException e) {
      e.printStackTrace();
      return;
    } catch (FileNotFoundException e) {
      e.printStackTrace();
      return;
    }
    JSONObject object = (JSONObject) tokener.nextValue();
    JSONObject request = object.getJSONObject("request");
    String requestBody = request.getString("body");
    requestBody = requestBody.replaceAll("@UUID", UUID.randomUUID().toString());
    String requestTopic = request.getString("topic");
    ServicePlatformMessage sp = new ServicePlatformMessage(requestBody, "application/xyaml",
        requestTopic, uuid, requestTopic);
    try {
      this.muxQueue.put(sp);
    } catch (InterruptedException e) {
      e.printStackTrace();
      return;
    }
    ProducerServer.incRequestsSent();
  }

  public int getThreadIndex() {
    return threadIndex;
  }

}
