package quux00.wordcount.kafka;

import quux00.wordcount.acking.ReportBolt;
import quux00.wordcount.acking.SplitSentenceBolt;
import quux00.wordcount.acking.WordCountBolt;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.ZkHosts;
import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

public class WordCountAckedTopology {

  private static final String SENTENCE_SPOUT_ID = "kafka-sentence-spout";
  private static final String SPLIT_BOLT_ID = "acking-split-bolt";
  private static final String COUNT_BOLT_ID = "acking-count-bolt";
  private static final String REPORT_BOLT_ID = "acking-report-bolt";
  private static final String TOPOLOGY_NAME = "acking-word-count-topology";

  public static void main(String[] args) throws Exception {
    int numSpoutExecutors = 1;
    
    KafkaSpout kspout = buildKafkaSentenceSpout();
    SplitSentenceBolt splitBolt = new SplitSentenceBolt();
    WordCountBolt countBolt = new WordCountBolt();
    ReportBolt reportBolt = new ReportBolt();
    
    TopologyBuilder builder = new TopologyBuilder();
    
    builder.setSpout(SENTENCE_SPOUT_ID, kspout, numSpoutExecutors);
    builder.setBolt(SPLIT_BOLT_ID, splitBolt).shuffleGrouping(SENTENCE_SPOUT_ID);
    builder.setBolt(COUNT_BOLT_ID, countBolt).fieldsGrouping(SPLIT_BOLT_ID, new Fields("word"));
    builder.setBolt(REPORT_BOLT_ID, reportBolt).globalGrouping(COUNT_BOLT_ID);
    
    Config cfg = new Config();    
    StormSubmitter.submitTopology(TOPOLOGY_NAME, cfg, builder.createTopology());
  }

  private static KafkaSpout buildKafkaSentenceSpout() {
    String zkHostPort = "localhost:2181";
    String topic = "sentences";

    String zkRoot = "/acking-kafka-sentence-spout";
    String zkSpoutId = "acking-sentence-spout";
    ZkHosts zkHosts = new ZkHosts(zkHostPort);
    
    SpoutConfig spoutCfg = new SpoutConfig(zkHosts, topic, zkRoot, zkSpoutId);
    KafkaSpout kafkaSpout = new KafkaSpout(spoutCfg);
    return kafkaSpout;
  }
}
