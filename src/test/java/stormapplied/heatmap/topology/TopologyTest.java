package stormapplied.heatmap.topology;

import backtype.storm.Config;
import backtype.storm.ILocalCluster;
import backtype.storm.testing.*;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import com.google.code.geocoder.model.LatLng;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Map;

import static backtype.storm.Testing.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static stormapplied.heatmap.topology.StormTopologyBuilder.*;

public class TopologyTest {
  private static final String FEEDER_SPOUT_ID = "feeder-spout";

  @Test
  public void verifyProperValuesAreEmittedByEachBolt() {
    MkClusterParam clusterParam = new MkClusterParam();
    clusterParam.setSupervisors(1);

    withSimulatedTimeLocalCluster(clusterParam, new TestJob() {
      @Override
      public void run(ILocalCluster cluster) {
        MockedSources mockedSources = new MockedSources();
        mockedSources.addMockData(CHECKINS_ID, new Values(1382904793783L, "287 Hudson St New York NY 10013"));

        Config config = new Config();
        config.setDebug(true);

        CompleteTopologyParam topologyParam = new CompleteTopologyParam();
        topologyParam.setMockedSources(mockedSources);
        topologyParam.setStormConf(config);

        LatLng latLng = new LatLng("40.725612", "-74.00791599999999");

        ArrayList<LatLng> latLngList = new ArrayList<LatLng>();
        latLngList.add(latLng);

        Map result = completeTopology(cluster, StormTopologyBuilder.build(), topologyParam);
        assertTrue(multiseteq(new Values(new Values(1382904793783L, "287 Hudson St New York NY 10013")), readTuples(result, CHECKINS_ID)));
        assertTrue(multiseteq(new Values(new Values(1382904793783L, latLng)), readTuples(result, GEOCODE_LOOKUP_ID)));
        assertTrue(multiseteq(new Values(new Values(92193652L, latLngList)), readTuples(result, HEATMAP_BUILDER_ID)));
        assertTrue(multiseteq(new Values(), readTuples(result, PERSISTOR_ID)));
      }
    });
  }

  @Test
  public void verifySpoutTuplesAreFullyAcked() {
    withTrackedCluster(new TestJob() {
      @Override
      public void run(ILocalCluster cluster) throws Exception {
        AckTracker ackTracker = new AckTracker();

        FeederSpout feederSpout = new FeederSpout(new Fields("time", "address"));
        feederSpout.setAckFailDelegate(ackTracker);

        TrackedTopology trackedTopology = mkTrackedTopology(cluster, StormTopologyBuilder.buildWithSpout(FEEDER_SPOUT_ID, feederSpout));
        cluster.submitTopology("test-acking", new Config(), trackedTopology.getTopology());

        feederSpout.feed(new Values(1382904793783L, "287 Hudson St New York NY 10013"));
        feederSpout.feed(new Values(1382904793783L, "287 Hudson St New York NY 10013"));
        feederSpout.feed(new Values(1382904793783L, "287 Hudson St New York NY 10013"));
        feederSpout.feed(new Values(1382904793783L, "287 Hudson St New York NY 10013"));
        trackedWait(trackedTopology, 4);

        assertEquals(4, ackTracker.getNumAcks());
        ackTracker.resetNumAcks();

        feederSpout.feed(new Values(1382904793783L, "287 Hudson St New York NY 10013"));
        trackedWait(trackedTopology, 1);
        assertEquals(1, ackTracker.getNumAcks());
        ackTracker.resetNumAcks();
      }
    });
  }
}