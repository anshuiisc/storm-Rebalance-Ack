package org.apache.storm.starter.storm.topo.rebalance;

//import org.apache.storm.starter.bolt.LatencyConfig;
//import org.apache.storm.starter.bolt.operation.Operations;
//import in.dream_lab.bm.stream_iot.storm.bolts.boltsUidai.LatencyConfig;
//import in.dream_lab.bm.stream_iot.storm.bolts.boltsUidai.operation.Operations;
import org.apache.storm.starter.storm.bolts.boltsUidai.LatencyConfig;
import org.apache.storm.starter.storm.bolts.boltsUidai.operation.Operations;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;

/**
 * Created by anshushukla on 28/02/17.
 */

//public class fooXMLParser extends OurStatefulBoltByteArrayTuple<String,List<byte[]>> {
public class fooXMLParser    extends BaseRichBolt {

    String inputFileString=null;
    String name;
//    KeyValueState<String, List<Object>> kvState;
    long sum;
//    public static String traceVal;
    fooXMLParser(String name) {
        this.name = name;
    }
    OutputCollector collector;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        System.out.println("TEST:prepare");
        this.collector = collector;
//        _context=context;

//        xml file specific code
        //        String inputXMLpath="/home/anshu/data/storm/dataset/tempSAX.xml";
        String inputXMLpath="/Users/anshushukla/Downloads/Storm/storm-1.0.3/examples/storm-starter/src/jvm/org/apache/storm/starter/bolt/operation/tempSAX.xml";

        try {
            inputFileString= LatencyConfig.readFileWithSize(inputXMLpath, StandardCharsets.UTF_8);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }



    @Override
    public void execute(Tuple input) {

        // user code
        String failFlag= (input.getValueByField("failFlag")).toString();
        String afterRebFlag= (input.getValueByField("afterRebFlag")).toString();
        String msgid=input.getValueByField("MSGID").toString();

//        Utils.sleep(3000);

        int tot_length = 0;
        for(int i=0;i<3;i++)
            tot_length += Operations.doXMLparseOp(inputFileString);

        Values out=  new Values(failFlag,afterRebFlag,msgid);
//        Utils.sleep(2000);

        collector.emit(input,out);
//            System.out.println("ACKING_from_bolt_MSGID"+input.getStringByField("MSGID"));

//            if(Long.valueOf(msgid)%2==0)
            collector.ack(input);



    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("failFlag","afterRebFlag", "MSGID"));
//        declarer.declare(new Fields("value","MSGID"));
    }

}

