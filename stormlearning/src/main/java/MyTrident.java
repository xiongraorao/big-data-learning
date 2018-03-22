import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.LocalDRPC;
import org.apache.storm.trident.TridentState;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.operation.builtin.Count;
import org.apache.storm.trident.operation.builtin.FilterNull;
import org.apache.storm.trident.operation.builtin.MapGet;
import org.apache.storm.trident.operation.builtin.Sum;
import org.apache.storm.trident.testing.FixedBatchSpout;
import org.apache.storm.trident.testing.MemoryMapState;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

/**
 * .
 *
 * @author Xiong Raorao
 * @since 2018-03-22-9:21
 */
public class MyTrident {

  public static void main(String[] args) throws InterruptedException {
    FixedBatchSpout spout = new FixedBatchSpout(new Fields("sentences"), 3,
        new Values("the cow jump over the moon"),
        new Values("the man went to the store and bought some candy"),
        new Values("four score and seven years ago"),
        new Values("how many apples can you eat"));
    spout.setCycle(true);
    TridentTopology topology = new TridentTopology();
    LocalDRPC localDRPC = new LocalDRPC();
    TridentState state = topology.newStream("spout", spout).parallelismHint(3)
        .each(new Fields("sentences"),
            new Split(), new Fields("word")).groupBy(new Fields("word"))
        .persistentAggregate(new MemoryMapState.Factory(), new Count(), new Fields("count"))
        .parallelismHint(6);

    topology.newDRPCStream("words", localDRPC)
        .each(new Fields("args"), new Split(), new Fields("word")).groupBy(new Fields(
        "word")).stateQuery(state, new Fields("word"), new MapGet(), new Fields("count"))
        .each(new Fields("count"),
            new FilterNull()).aggregate(new Fields("count"), new Sum(), new Fields("sum"));

    LocalCluster cluster = new LocalCluster();
    Config conf = new Config();
    conf.setMaxSpoutPending(20);
    cluster.submitTopology("wordCount", conf, topology.build());

    for (int i = 0; i < 100; i++) {
      System.out.println("DRPC RESULT: " + localDRPC.execute("words", "cat the dog jumped"));
      Thread.sleep(1000);
    }

  }

  static class Split extends BaseFunction {

    @Override
    public void execute(TridentTuple tridentTuple, TridentCollector tridentCollector) {
      for (String word : tridentTuple.getString(0).split(" ")) {
        tridentCollector.emit(new Values(word));
      }
    }
  }

}
