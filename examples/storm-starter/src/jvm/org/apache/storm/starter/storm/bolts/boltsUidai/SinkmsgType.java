package org.apache.storm.starter.storm.bolts.boltsUidai;

//import in.dream_lab.bm.stream_iot.storm.genevents.logging.BatchedFileLogging;
import org.apache.storm.starter.storm.genevents.logging.BatchedFileLogging;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.lang.management.ManagementFactory;
import java.util.Map;

/**
 * Created by anshushukla on 19/05/15.
 */
public class SinkmsgType extends BaseRichBolt {

    OutputCollector collector;
    BatchedFileLogging ba;
    String csvFileNameOutSink;  //Full path name of the file at the sink bolt

    public SinkmsgType(){

    }

    public SinkmsgType(String csvFileNameOutSink){
        this.csvFileNameOutSink = csvFileNameOutSink;
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector=outputCollector;
        BatchedFileLogging.writeToTemp(this, this.csvFileNameOutSink);
         //ba=new BatchedFileLogging();
        ba=new BatchedFileLogging(this.csvFileNameOutSink, topologyContext.getThisComponentId());

        System.out.println("SinkBolt PID,"+ ManagementFactory.getRuntimeMXBean().getName());
    }

    @Override
    public void execute(Tuple input) {
        String msgId = input.getStringByField("MSGID");
        String exe_time = input.getStringByField("time");  //addon
        System.out.println("exe_time-"+exe_time);
        //collector.emit(input,new Values(msgId));
        try {
 //        ba.batchLogwriter(System.nanoTime(),msgId);
 ba.batchLogwriter(System.currentTimeMillis(),msgId+","+exe_time);//addon
        } catch (Exception e) {
            e.printStackTrace();
        }
        //collector.ack(input);
    }



    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
