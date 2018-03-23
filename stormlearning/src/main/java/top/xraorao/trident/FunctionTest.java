/*
 * Copyright (c) 2018.  Xiong Raorao. All rights reserved.
 * Project Name: big-data-learning
 * File Name: FunctionTest.java
 * Date: 18-3-23 上午11:11
 * Author: Xiong Raorao
 */

package top.xraorao.trident;

import java.util.List;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.testing.FixedBatchSpout;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import top.xraorao.util.FileLogger;

/**
 * .
 *
 * @author Xiong Raorao
 * @since 2018-03-23-11:11
 */
public class FunctionTest {

  private static FileLogger logger = new FileLogger("./fuctionTest.log");

  public static void main(String[] args)
      throws InvalidTopologyException, AuthorizationException, AlreadyAliveException {
    TridentTopology topology = new TridentTopology();
    FixedBatchSpout spout = new FixedBatchSpout(new Fields("a", "b", "c"), 1, new Values(1, 2, 3),
        new Values(4, 1, 6), new Values(3, 0, 8));
    spout.setCycle(false);
    topology.newStream("spout", spout)
        .each(new Fields("a", "b", "c"), new MyFunction(), new Fields("d"))
        .each(new Fields("a", "b", "c", "d"), new PrintFunction(), new Fields(""));

    logger.log("INFO", "start submit topology");
    //LocalCluster cluster = new LocalCluster();
    //cluster.submitTopology("function test", new Config(), topology.build());
    StormSubmitter.submitTopology("function-test", new Config(), topology.build());

  }

  static class PrintFunction extends BaseFunction {

    @Override
    public void execute(TridentTuple tridentTuple, TridentCollector tridentCollector) {
      List<Object> objectList = tridentTuple.getValues();
      StringBuilder sb = new StringBuilder();
      sb.append("[");
      for (Object o : objectList) {
        sb.append(o + ", ");
      }
      sb.append("]");
      System.out.println(sb.toString());
      logger.log("INFO", "list: " + sb.toString());
    }
  }

  static class MyFunction extends BaseFunction {

    @Override
    public void execute(TridentTuple tridentTuple, TridentCollector tridentCollector) {
      for (int i = 0; i < tridentTuple.getInteger(0); i++) {
        tridentCollector.emit(new Values(i));
      }
    }
  }

}
