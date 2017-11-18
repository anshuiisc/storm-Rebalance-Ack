package org.apache.storm.starter.storm.spouts;


//import in.dream_lab.bm.stream_iot.storm.genevents.EventGen;
//import in.dream_lab.bm.stream_iot.storm.genevents.ISyntheticEventGen;
//import in.dream_lab.bm.stream_iot.storm.genevents.logging.BatchedFileLogging;
//import in.dream_lab.bm.stream_iot.storm.genevents.utils.GlobalConstants;
import com.google.common.base.Stopwatch;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.starter.storm.genevents.EventGen;
import org.apache.storm.starter.storm.genevents.ISyntheticEventGen;
import org.apache.storm.starter.storm.genevents.logging.BatchedFileLogging;
import org.apache.storm.starter.storm.genevents.utils.GlobalConstants;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

//import org.apache.storm.spout.OurCheckpointSpout;

public class SampleSpoutWithAcking extends BaseRichSpout implements ISyntheticEventGen {
	SpoutOutputCollector _collector;
	EventGen eventGen;
	BlockingQueue<List<String>> eventQueue;
	String csvFileName;
	String outSpoutCSVLogFileName;
	String experiRunId;
	double scalingFactor;
	BatchedFileLogging ba;
	long msgId;
	public HashMap<Long, Values> pending;
	public int afterRebFlag=0;
	public int failFlag=0;
	private Long sequence = Long.MIN_VALUE;

	private Long nextId(){
		this.sequence++;
		if(this.sequence == Long.MAX_VALUE){
			this.sequence = Long.MIN_VALUE;
		}
		return this.sequence;
	}

	public SampleSpoutWithAcking(){
//		this.csvFileName = "/home/ubuntu/sample100_sense.csv";
//		System.out.println("Inside  sample spout code");
		this.csvFileName = "/home/tarun/j2ee_workspace/eventGen-anshu/eventGen/bangalore.csv";
		this.scalingFactor = GlobalConstants.accFactor;
//		System.out.print("the output is as follows");
	}

	public SampleSpoutWithAcking(String csvFileName, String outSpoutCSVLogFileName, double scalingFactor, String experiRunId){
		this.csvFileName = csvFileName;
		this.outSpoutCSVLogFileName = outSpoutCSVLogFileName;
		this.scalingFactor = scalingFactor;
		this.experiRunId = experiRunId;
	}

	public SampleSpoutWithAcking(String csvFileName, String outSpoutCSVLogFileName, double scalingFactor){
		this(csvFileName, outSpoutCSVLogFileName, scalingFactor, "");
	}



	@Override
	public void open(Map map, TopologyContext context, SpoutOutputCollector collector) {
		// TODO Auto-generated method stub
//		System.out.println("SampleSpout PID,"+ ManagementFactory.getRuntimeMXBean().getName());
		BatchedFileLogging.writeToTemp(this,this.outSpoutCSVLogFileName);
		Random r=new Random();

		this.pending = new HashMap<Long, Values>();

		try {
			if(InetAddress.getLocalHost().getHostName().compareTo("anshuStormSCsup1")==0)
				msgId= (long) (1*Math.pow(10,12)+(r.nextInt(1000)*Math.pow(10,9))+r.nextInt(10));
			if(InetAddress.getLocalHost().getHostName().compareTo("anshuStormSCsup2")==0)
				msgId= (long) (2*Math.pow(10,12)+(r.nextInt(1000)*Math.pow(10,9))+r.nextInt(10));
			if(InetAddress.getLocalHost().getHostName().compareTo("anshuStormSCsup3")==0)
				msgId= (long) (3*Math.pow(10,12)+(r.nextInt(1000)*Math.pow(10,9))+r.nextInt(10));
			if(InetAddress.getLocalHost().getHostName().compareTo("anshuStormSCsup4")==0)
				msgId= (long) (4*Math.pow(10,12)+(r.nextInt(1000)*Math.pow(10,9))+r.nextInt(10));
			if(InetAddress.getLocalHost().getHostName().compareTo("anshuStormSCsup5")==0)
				msgId= (long) (5*Math.pow(10,12)+(r.nextInt(1000)*Math.pow(10,9))+r.nextInt(10));
			if(InetAddress.getLocalHost().getHostName().compareTo("anshuStormSCsup6")==0)
				msgId= (long) (6*Math.pow(10,12)+(r.nextInt(1000)*Math.pow(10,9))+r.nextInt(10));
			if(InetAddress.getLocalHost().getHostName().compareTo("anshuStormSCsup7")==0)
				msgId= (long) (7*Math.pow(10,12)+(r.nextInt(1000)*Math.pow(10,9))+r.nextInt(10));
			if(InetAddress.getLocalHost().getHostName().compareTo("anshuStormSCsup8")==0)
				msgId= (long) (8*Math.pow(10,12)+(r.nextInt(1000)*Math.pow(10,9))+r.nextInt(10));
			if(InetAddress.getLocalHost().getHostName().compareTo("anshuStormSCsup9")==0)
				msgId= (long) (9*Math.pow(10,12)+(r.nextInt(1000)*Math.pow(10,9))+r.nextInt(10));
			if(InetAddress.getLocalHost().getHostName().compareTo("anshuStormSCsup10")==0)
				msgId= (long) (10*Math.pow(10,12)+(r.nextInt(1000)*Math.pow(10,9))+r.nextInt(10));
			if(InetAddress.getLocalHost().getHostName().compareTo("anshuStormSCsup11")==0)
				msgId= (long) (11*Math.pow(10,12)+(r.nextInt(1000)*Math.pow(10,9))+r.nextInt(10));
			if(InetAddress.getLocalHost().getHostName().compareTo("anshuStormSCsup12")==0)
				msgId= (long) (12*Math.pow(10,12)+(r.nextInt(1000)*Math.pow(10,9))+r.nextInt(10));
			if(InetAddress.getLocalHost().getHostName().compareTo("anshuStormSCsup13")==0)
				msgId= (long) (13*Math.pow(10,12)+(r.nextInt(1000)*Math.pow(10,9))+r.nextInt(10));
			if(InetAddress.getLocalHost().getHostName().compareTo("anshuStormSCsup14")==0)
				msgId= (long) (14*Math.pow(10,12)+(r.nextInt(1000)*Math.pow(10,9))+r.nextInt(10));
			if(InetAddress.getLocalHost().getHostName().compareTo("anshuStormSCsup15")==0)
				msgId= (long) (15*Math.pow(10,12)+(r.nextInt(1000)*Math.pow(10,9))+r.nextInt(10));
			if(InetAddress.getLocalHost().getHostName().compareTo("anshuStormSCsup16")==0)
				msgId= (long) (16*Math.pow(10,12)+(r.nextInt(1000)*Math.pow(10,9))+r.nextInt(10));
			if(InetAddress.getLocalHost().getHostName().compareTo("anshuStormSCsup17")==0)
				msgId= (long) (17*Math.pow(10,12)+(r.nextInt(1000)*Math.pow(10,9))+r.nextInt(10));
			if(InetAddress.getLocalHost().getHostName().compareTo("anshuStormSCsup18")==0)
				msgId= (long) (18*Math.pow(10,12)+(r.nextInt(1000)*Math.pow(10,9))+r.nextInt(10));
			if(InetAddress.getLocalHost().getHostName().compareTo("anshuStormSCsup19")==0)
				msgId= (long) (19*Math.pow(10,12)+(r.nextInt(1000)*Math.pow(10,9))+r.nextInt(10));
			if(InetAddress.getLocalHost().getHostName().compareTo("anshuStormSCsup20")==0)
				msgId= (long) (20*Math.pow(10,12)+(r.nextInt(1000)*Math.pow(10,9))+r.nextInt(10));
			if(InetAddress.getLocalHost().getHostName().compareTo("anshuStormSCsup21")==0)
				msgId= (long) (21*Math.pow(10,12)+(r.nextInt(1000)*Math.pow(10,9))+r.nextInt(10));
			if(InetAddress.getLocalHost().getHostName().compareTo("anshuStormSCsup22")==0)
				msgId= (long) (22*Math.pow(10,12)+(r.nextInt(1000)*Math.pow(10,9))+r.nextInt(10));
			if(InetAddress.getLocalHost().getHostName().compareTo("anshuStormSCsup23")==0)
				msgId= (long) (23*Math.pow(10,12)+(r.nextInt(1000)*Math.pow(10,9))+r.nextInt(10));
			if(InetAddress.getLocalHost().getHostName().compareTo("anshuStormSCsup24")==0)
				msgId= (long) (24*Math.pow(10,12)+(r.nextInt(1000)*Math.pow(10,9))+r.nextInt(10));
			if(InetAddress.getLocalHost().getHostName().compareTo("Anshus-MacBook-Pro.local")==0)
				msgId= (long) (24*Math.pow(10,12)+(r.nextInt(1000)*Math.pow(10,9))+r.nextInt(10));
			if(InetAddress.getLocalHost().getHostName().compareTo("anshudreamD")==0)
				msgId= (long) (24*Math.pow(10,12)+(r.nextInt(1000)*Math.pow(10,11))+r.nextInt(10));

			else
				msgId= (long) (r.nextInt(100)*Math.pow(10,12)+(r.nextInt(1000)*Math.pow(10,11))+r.nextInt(10));

//			else
//					msgId=r.nextInt(10000);


		} catch (UnknownHostException e) {

			e.printStackTrace();
		}



//		msgId=r.nextInt(10000);
		_collector = collector;
		this.eventGen = new EventGen(this,this.scalingFactor);
		this.eventQueue = new LinkedBlockingQueue<List<String>>();
		String uLogfilename=this.outSpoutCSVLogFileName+msgId;
		this.eventGen.launch(this.csvFileName, uLogfilename); //Launch threads

		ba=new BatchedFileLogging(uLogfilename, context.getThisComponentId());


	}


	@Override
	public void nextTuple() {
		// TODO Auto-generated method stub
//		try {
//		System.out.println("spout Queue count= "+this.eventQueue.size());
		// allow multiple tuples to be emitted per next tuple.
		// Discouraged? https://groups.google.com/forum/#!topic/storm-user/SGwih7vPiDE
		int count = 0, MAX_COUNT=10; // FIXME?
//		while(count < MAX_COUNT) {
			List<String> entry = this.eventQueue.poll(); // nextTuple should not block!

			if(entry == null) return;
			count++;

			StringBuilder rowStringBuf = new StringBuilder();
			for(String s : entry){
				rowStringBuf.append(",").append(s);
			}
			String rowString = rowStringBuf.toString().substring(1);
//			String rowString = rowStringBuf.toString();
//			values.add(rowString);
			msgId++;
//			values.add(Long.toString(msgId));


			/// checking for file created by bash script to show start rebalance
//			File f2 = new File(GlobalConstants.BASE_SIGNAL_DIR_PATH +"LastCheckpointAck");
//			if(f2.exists()){
//				System.out.println("###########_Got_SIGNAL_to_start_CHKPT_###########");
//				logTimeStamp("ACK_COMMIT,"+System.currentTimeMillis(),"LOGTS");
//				if(f2.delete()){
//					System.out.println(f2.getName() + " is deleted!");
//				}
//				Utils.sleep(33000);  // rebalance time
//			}
//
			/// checking for file created by bash script jsut after rebalance command to specify message type
			File f3 = new File(GlobalConstants.BASE_SIGNAL_DIR_PATH +"RECOVERSTATE");
			if(f3.exists()){
				logTimeStamp("RECOVERSTATE,"+System.currentTimeMillis(),"LOGTS");
				System.out.println("###########_Got_SIGNAL_to_RECOVERSTATE_###########");
				if(f3.delete()){
					System.out.println(f3.getName() + " is deleted!");
				}
//				val=1;
				afterRebFlag=1;
			}
			Values values = new Values();
//			values.add(rowString);
			values.add(failFlag);
			values.add(afterRebFlag);
			values.add(msgId);
			
			this._collector.emit(values,msgId);
			this.pending.put(msgId,values);
			try {
					ba.batchLogwriter(System.currentTimeMillis(),"MSGID"+0+"_"+afterRebFlag+"," + msgId);
			} catch (Exception e) {
				e.printStackTrace();
			}
	}


	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		//List<String> fieldsList = EventGen.getHeadersFromCSV(csvFileName);
		//fieldsList.add("MSGID");
		//declarer.declare(new Fields(fieldsList));
		declarer.declare(new Fields("failFlag","afterRebFlag", "MSGID"));
	}


	/**
	 * Storm has determined that the tuple emitted by this spout with the msgId identifier
	 * has been fully processed. Typically, an implementation of this method will take that
	 * message off the queue and prevent it from being replayed.
	 *
	 * @param msgId
	 */
	public void ack(Object msgId) {
//		System.out.println("removing_message_id_from_hashmap:"+msgId);
		 pending.remove(msgId);
		logTimeStamp(System.currentTimeMillis()+","+pending.size(),"ACKED");
	}

	/**
	 * The tuple emitted by this spout with the msgId identifier has failed to be
	 * fully processed. Typically, an implementation of this method will put that
	 * message back on the queue to be replayed at a later time.
	 *
	 * @param msgId
	 */
	public void fail(Object msgId) {
//		failFlag=1;
//		afterRebFlag=1;
//		System.out.println("Re-emitting_values_from_HashMap:"+msgId+","+pending.get(msgId));

//		this._collector.emit(new Values(failFlag,afterRebFlag,pending.get(msgId).get(1)),msgId);
		this._collector.emit(new Values(1,afterRebFlag,msgId),msgId);
		try {
			ba.batchLogwriter(System.currentTimeMillis(),"MSGID"+1+"_"+afterRebFlag+"," + msgId);
		} catch (Exception e) {
			e.printStackTrace();
		}
		logTimeStamp("REPLAYED,"+msgId+","+System.currentTimeMillis(),"FAIL");
	}



	@Override
	public void receive(List<String> event) {
		// TODO Auto-generated method stub
		//System.out.println("Called IN SPOUT### ");
		try {
			this.eventQueue.put(event);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	// used for logging only
	public static void  logTimeStamp(String s,String file){
		try
		{
			String filename= GlobalConstants.BASE_SIGNAL_DIR_PATH +file;
			FileWriter fw = new FileWriter(filename,true); //the true will append the new data
			fw.write(s+"\n");//appends the string to the file
			fw.close();
		}
		catch(IOException ioe)
		{
			System.err.println("IOException: " + ioe.getMessage());
		}
	}
}
