package stormapplied.heatmap.topology;

import backtype.storm.Config;
import backtype.storm.Constants;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.google.code.geocoder.model.LatLng;

import java.util.*;

public class HeatMapBuilder extends BaseBasicBolt {
  private Map<Long, List<LatLng>> heatmaps;

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("time-interval", "hotzones"));
  }

  @Override
  public void prepare(Map stormConf,
                      TopologyContext context) {
    heatmaps = new HashMap<Long, List<LatLng>>();
  }

  @Override
  public Map<String, Object> getComponentConfiguration() {
    Config conf = new Config();
    conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, 60);
    return conf;
  }


  @Override
  public void execute(Tuple tuple,
                      BasicOutputCollector outputCollector) {
    if (isTickTuple(tuple)) {
      emitHeatmap(outputCollector);
    } else {
      Long time = tuple.getLongByField("time");
      LatLng geocode = (LatLng) tuple.getValueByField("geocode");

      Long timeInterval = selectTimeInterval(time);
      List<LatLng> checkins = getCheckinsForInterval(timeInterval);
      checkins.add(geocode);
    }
  }

  private boolean isTickTuple(Tuple tuple) {
    String sourceComponent = tuple.getSourceComponent();
    String sourceStreamId = tuple.getSourceStreamId();
    return sourceComponent.equals(Constants.SYSTEM_COMPONENT_ID)
        && sourceStreamId.equals(Constants.SYSTEM_TICK_STREAM_ID);
  }

  private void emitHeatmap(BasicOutputCollector outputCollector) {
    Long now = System.currentTimeMillis();
    Long emitUpToTimeInterval = selectTimeInterval(now);
    Set<Long> timeIntervalsAvailable = heatmaps.keySet();
    for (Long timeInterval : timeIntervalsAvailable) {
      if (timeInterval <= emitUpToTimeInterval) {
        List<LatLng> hotzones = heatmaps.remove(timeInterval);
        outputCollector.emit(new Values(timeInterval, hotzones));
      }
    }
  }

  private Long selectTimeInterval(Long time) {
    return time / (15 * 1000);
  }

  private List<LatLng> getCheckinsForInterval(Long timeInterval) {
    List<LatLng> hotzones = heatmaps.get(timeInterval);
    if (hotzones == null) {
      hotzones = new ArrayList<LatLng>();
      heatmaps.put(timeInterval, hotzones);
    }
    return hotzones;
  }
}
