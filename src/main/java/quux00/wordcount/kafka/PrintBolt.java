package quux00.wordcount.kafka;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.util.List;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

public class PrintBolt extends BaseRichBolt  {

	private static final long serialVersionUID = 1L;


   private OutputCollector collector;
   public void prepare(Map cfg, TopologyContext topologyCtx, OutputCollector outCollector)
	  {
	    collector = outCollector;
     }
	  public void declareOutputFields(OutputFieldsDeclarer declarer) 
       {
		    
		  declarer.declare(new Fields("json_data"));
	  }
	  
	  public void execute(Tuple tuple)
	  {
		  
		    Object value = tuple.getValue(0);
		    String rawtuple = null;
		    if (value instanceof String) {
		      rawtuple = (String) value;

		    } else {
		      // Kafka returns bytes
		      byte[] bytes = (byte[]) value;
		      try {
		    	  rawtuple = new String(bytes, "UTF-8");
		      }
            catch (UnsupportedEncodingException e) {
		        throw new RuntimeException(e);
		      }      
		    }

		    JSONObject json_data = (JSONObject)JSONValue.parse(rawtuple);
		    System.out.println("json_data: "+json_data);
		    
		    collector.emit(new Values(json_data));
		    collector.ack(tuple);
		  }
	

}
