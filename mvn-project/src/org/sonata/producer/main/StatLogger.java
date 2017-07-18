package org.sonata.producer.main;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;

public class StatLogger {

  private File logFile;
  private static StatLogger myInstance = null;
  protected final static String filePath = "./stats.csv";

  private StatLogger() {
    logFile = new File(StatLogger.filePath);

    try {
      logFile.createNewFile();
    } catch (IOException e1) {
      // TODO Auto-generated catch block
      e1.printStackTrace();
    }
  }

  public static StatLogger getInstance() {
    if (myInstance == null) myInstance = new StatLogger();
    return myInstance;
  }


  public void writeLine(long delay, String type, boolean success) {
    try {
      FileOutputStream out = new FileOutputStream(logFile, true);
      
      String line = String.format("%d,%s,%d%n", delay, type, success?1:0);
      
      out.write(line.getBytes());
      
    } catch (FileNotFoundException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    }

  }

}
