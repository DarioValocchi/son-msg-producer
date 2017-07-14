package org.sonata.producer.main;

import java.io.File;
import java.net.URL;

import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.impl.Arguments;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;

public class ProducerCLI {


  public static void main(String[] args) {
    ArgumentParser parser = ArgumentParsers.newArgumentParser("RabbitProducer")
        .description("Generate a number of AMQP broker.").defaultHelp(true).epilog("for queries and help d.valocchi@ucl.ac.uk");
    parser.addArgument("--url").type(String.class).metavar("-u").nargs("?")
        .setDefault("amqp://guest:guest@localhost:5672/")
        .help("The URL of the AMQP broker");
    parser.addArgument("--threads").type(Integer.class).metavar("-t").setDefault(1)
        .help("The number of producer to start").nargs("?");
    parser.addArgument("--folder").type(File.class).metavar("-f").nargs("?")
        .help("Folder from which message payload are randomly taken.\n"
            + "The folder must contain .req file. Each file is structured as a JSON in the format:\n"
            + "{\n" 
            + "  request:{\n"
            + "    body:String\n"
            + "    topic:String\n"
            + "  }\n" 
            + "  response:{\n" 
            + "    key_to_read:String\n"
            + "    expected_value:String\n" 
            + "  }\n" 
            + "}\n")
        .setDefault(new File("./messages"));
    parser.addArgument("--exchange").type(String.class).metavar("-x").nargs("?")
        .setDefault("son-kernel").help("Name of the exchange");
    parser.addArgument("--requests").type(Integer.class).metavar("-r").nargs("?")
          .setDefault(10).help("Number of random requests sent per thread");
    try {
      Namespace res = parser.parseArgs(args);
      ProducerServer server = new ProducerServer(res);
      server.start();
    } catch (ArgumentParserException e) {
      parser.printHelp();
      parser.handleError(e);
    }
    
    
    
  }


}
