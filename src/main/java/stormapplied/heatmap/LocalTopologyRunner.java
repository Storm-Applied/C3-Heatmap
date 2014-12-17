package stormapplied.heatmap;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.utils.Utils;
import stormapplied.heatmap.topology.StormTopologyBuilder;

public class LocalTopologyRunner {
  private static final int TEN_MINUTES = 600000;

  public static void main(String[] args) {
    Config config = new Config();
    config.setDebug(true);

    LocalCluster cluster = new LocalCluster();
    cluster.submitTopology(StormTopologyBuilder.TOPOLOGY_NAME, config, StormTopologyBuilder.build());

    Utils.sleep(TEN_MINUTES);
    cluster.killTopology(StormTopologyBuilder.TOPOLOGY_NAME);
    cluster.shutdown();
  }
}