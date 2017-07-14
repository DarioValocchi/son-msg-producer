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

import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONTokener;
import org.sonata.producer.rabbit.ServicePlatformMessage;

import com.esotericsoftware.yamlbeans.YamlException;
import com.esotericsoftware.yamlbeans.YamlReader;

public class Generator implements Runnable, Observer {

  private HashMap<String, Generator> lockMap;
  private int maxRequests;
  private File requestsFolder;
  private BlockingQueue<ServicePlatformMessage> muxQueue;
  private Random rand;
  private final Object lock;
  private String currentUuid = null;
  private File currentRequestFile = null;

  public Generator(int r, File f, HashMap<String, Generator> m,
      BlockingQueue<ServicePlatformMessage> muxQueue) {
    this.requestsFolder = f;
    this.maxRequests = r;
    this.lockMap = m;
    this.muxQueue = muxQueue;
    this.rand = new Random(System.currentTimeMillis());
    this.lock = new Object();
  }

  @Override
  public void run() {

    int numFile = requestsFolder.list().length;
    int requestDone = 0;
    while (requestDone < this.maxRequests) {
      currentUuid = UUID.randomUUID().toString();
      lockMap.put(currentUuid, this);
      sendRequest(currentUuid);
      try {
        lock.wait();
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
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
    JSONObject object = (JSONObject) tokener.nextValue();
    String expKey = object.getString("key_to_read");
    String expValue = object.getString("expected_status");

    if (response.getContentType().equals("application/json")) {
      tokener = new JSONTokener(response.getBody());
      object = (JSONObject) tokener.nextValue();
      String result = object.getString(expKey);
      if (!object.has(expKey) || !result.equals(expValue)) {
        ProducerServer.incRequestsFailed();
      }
    } else if (response.getContentType().equals("application/json")) {
      YamlReader reader = new YamlReader(response.getBody());
      Object obj;
      try {
        obj = reader.read();
        Map<String, Object> map = (Map<String, Object>) obj;
        if (!map.containsKey(expKey) || !map.get(expKey).equals(expValue)) {
          ProducerServer.incRequestsFailed();
        }
      } catch (YamlException e) {
        e.printStackTrace();
      }
    }
    lockMap.remove(currentUuid);
    currentUuid = null;
    currentRequestFile = null;
    lock.notify();
  }

  private void sendRequest(String uuid) {
    File[] fileList = this.requestsFolder.listFiles();
    int requestNumber = rand.nextInt(fileList.length);
    currentRequestFile = fileList[requestNumber];
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
    String requestBody = object.getString("body");
    String requestTopic = object.getString("topic");
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

}
