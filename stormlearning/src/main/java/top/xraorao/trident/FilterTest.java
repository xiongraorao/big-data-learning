/*
 * Copyright (c) 2018.  Xiong Raorao. All rights reserved.
 * Project Name: big-data-learning
 * File Name: FilterTest.java
 * Date: 18-3-22 下午8:00
 * Author: Xiong Raorao
 */

package top.xraorao.trident;

import java.util.ArrayList;
import java.util.List;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.BaseFilter;
import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.testing.FixedBatchSpout;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

/**
 * 测试Filter的使用.
 *
 * @author Xiong Raorao
 * @since 2018-03-22-20:00
 */
public class FilterTest {

  public static void main(String[] args) throws InterruptedException {
    FixedBatchSpout spout = new FixedBatchSpout(new Fields("a", "b"),
        1, new Values(1, 2), new Values(4, 1),
        new Values(3, 0));
    spout.setCycle(false);
    TridentTopology topology = new TridentTopology();
    topology.newStream("spout", spout)
        .each(new Fields("a"), new MyFilter())
        .each(new Fields("a", "b"), new PrintFilterBolt(), new Fields(""));
    Config config = new Config();
    config.setNumAckers(1);
    config.setDebug(false);
    LocalCluster cluster = new LocalCluster();
    cluster.submitTopology("filter test", config, topology.build());
    Thread.sleep(5000);
    cluster.shutdown();

  }

  static class PrintFilterBolt extends BaseFunction {

    @Override
    public void execute(TridentTuple tridentTuple, TridentCollector tridentCollector) {
      int fistIndex = tridentTuple.getInteger(0);
      int secondIndex = tridentTuple.getInteger(1);
      List<Integer> list = new ArrayList<>();
      list.add(fistIndex);
      list.add(secondIndex);
      System.out.println("after storm filter opertition change is : "
          + list.toString());
    }
  }

  static class MyFilter extends BaseFilter {

    @Override
    public boolean isKeep(TridentTuple tridentTuple) {
      // 保留第一个字段为1的tuple
      return tridentTuple.getInteger(0) == 1;
    }
  }

}
