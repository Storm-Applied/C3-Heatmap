package stormapplied.heatmap.topology;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.google.code.geocoder.Geocoder;
import com.google.code.geocoder.GeocoderRequestBuilder;
import com.google.code.geocoder.model.*;

import java.util.Map;

public class GeocodeLookup extends BaseBasicBolt {
  private Geocoder geocoder;

  @Override
  public void declareOutputFields(OutputFieldsDeclarer fieldsDeclarer) {
    fieldsDeclarer.declare(new Fields("time", "geocode"));
  }

  @Override
  public void prepare(Map stormConf,
                      TopologyContext context) {
    geocoder = new Geocoder();
  }

  @Override
  public void execute(Tuple tuple,
                      BasicOutputCollector outputCollector) {
    String address = tuple.getStringByField("address");
    Long time = tuple.getLongByField("time");

    GeocoderRequest request = new GeocoderRequestBuilder()
        .setAddress(address)
        .setLanguage("en")
        .getGeocoderRequest();
    GeocodeResponse response = geocoder.geocode(request);
    GeocoderStatus status = response.getStatus();
    if (GeocoderStatus.OK.equals(status)) {
      GeocoderResult firstResult = response.getResults().get(0);
      LatLng latLng = firstResult.getGeometry().getLocation();
      outputCollector.emit(new Values(time, latLng));
    }
  }
}
