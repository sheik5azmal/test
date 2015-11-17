package quux00.wordcount.kafka;

//import quux00.wordcount.nonacking.ReportBolt;
//import quux00.wordcount.nonacking.SplitSentenceBolt;
//import quux00.wordcount.nonacking.WordCountBolt;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.ZkHosts;
import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

public class WordCountNonAckedTopology {

   private static final String SENTENCE_SPOUT_ID = "kafka-sentence-spout";
   private static final String BOLT = "PrintBolt";
 //  private static final String COUNT_BOLT_ID = "count-bolt";
 //  private static final String REPORT_BOLT_ID = "report-bolt";
   private static final String TOPOLOGY_NAME = "KS-topology";

  public static void main(String[] args) throws Exception {
    int numSpoutExecutors = 1;
    
    KafkaSpout kspout = buildKafkaSentenceSpout();
   // SplitSentenceBolt splitBolt = new SplitSentenceBolt();
   // WordCountBolt countBolt = new WordCountBolt();
  //  ReportBolt reportBolt = new ReportBolt();
   // PrintBolt2 printbolt = new PrintBolt2();
    TopologyBuilder builder = new TopologyBuilder();
    
   // builder.setSpout(SENTENCE_SPOUT_ID, kspout, numSpoutExecutors);
    builder.setSpout(SENTENCE_SPOUT_ID, kspout, 1);
    builder.setBolt(BOLT, new PrintBolt()).shuffleGrouping(SENTENCE_SPOUT_ID);
    //builder.setBolt(SPLIT_BOLT_ID, splitBolt).shuffleGrouping(SENTENCE_SPOUT_ID);
   // builder.setBolt(COUNT_BOLT_ID, countBolt).fieldsGrouping(SPLIT_BOLT_ID, new Fields("word"));
   // builder.setBolt(REPORT_BOLT_ID, reportBolt).globalGrouping(COUNT_BOLT_ID);
    
    Config cfg = new Config();    
    StormSubmitter.submitTopology(TOPOLOGY_NAME, cfg, builder.createTopology());
  }

  private static KafkaSpout buildKafkaSentenceSpout() {
    String zkHostPort = "172.31.58.177:2181";
    String topic = "sensor-msg";

    String zkRoot = "/kafka-sentence-spout";
    String zkSpoutId = "sensor-msg-spout";
    ZkHosts zkHosts = new ZkHosts(zkHostPort);
    
    SpoutConfig spoutCfg = new SpoutConfig(zkHosts, topic, zkRoot, zkSpoutId);
    KafkaSpout kafkaSpout = new KafkaSpout(spoutCfg);
    return kafkaSpout;
  }
}
