/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.storm.starter.storm.topo.rebalance;

//import in.dream_lab.bm.stream_iot.storm.bolts.boltsUidai.Sink;
//import in.dream_lab.bm.stream_iot.storm.genevents.factory.ArgumentClass;
//import in.dream_lab.bm.stream_iot.storm.genevents.factory.ArgumentParser;
//import in.dream_lab.bm.stream_iot.storm.spouts.SampleSpoutWithAcking;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.starter.storm.bolts.boltsUidai.Sink;
import org.apache.storm.starter.storm.genevents.factory.ArgumentClass;
import org.apache.storm.starter.storm.genevents.factory.ArgumentParser;
import org.apache.storm.starter.storm.spouts.SampleSpoutWithAcking;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

//import org.apache.storm.starter.genevents.factory.ArgumentClass;
//import org.apache.storm.starter.genevents.factory.ArgumentParser;
//import org.apache.storm.starter.spout.SampleSpoutWithCHKPTSpout;
//import org.apache.storm.starter.spout.Sink;

/**
 * An example topology that demonstrates the use of {@link org.apache.storm.topology.IStatefulBolt}
 * to manage state. To run the example,
 * <pre>
 * $ storm jar examples/storm-starter/storm-starter-topologies-*.jar storm.starter.StatefulTopology statetopology
 * </pre>
 * <p/>
 * The default state used is 'InMemoryKeyValueState' which does not persist the state across restarts. You could use
 * 'RedisKeyValueState' to test state persistence by setting below property in conf/storm.yaml
 * <pre>
 * topology.state.provider: org.apache.storm.redis.state.RedisKeyValueStateProvider
 * </pre>
 * <p/>
 * You should also start a local redis instance before running the 'storm jar' command. The default
 * RedisKeyValueStateProvider parameters can be overridden in conf/storm.yaml, for e.g.
 * <p/>
 * <pre>
 * topology.state.provider.config: '{"keyClass":"...", "valueClass":"...",
 *                                   "keySerializerClass":"...", "valueSerializerClass":"...",
 *                                   "jedisPoolConfig":{"host":"localhost", "port":6379,
 *                                      "timeout":2000, "database":0, "password":"xyz"}}'
 *
 * </pre>
 * </p>
 */
public class FooLinearParseTopologySleep {
    private static final Logger LOG = LoggerFactory.getLogger(FooLinearParseTopologySleep.class);

    public static void main(String[] args) throws Exception {

        /** Common Code begins **/
        ArgumentClass argumentClass = ArgumentParser.parserCLI(args);
        if (argumentClass == null) {
            System.out.println("ERROR! INVALID NUMBER OF ARGUMENTS");
            return;
        }


        String logFilePrefix = argumentClass.getTopoName() + "-" + argumentClass.getExperiRunId() + "-" + argumentClass.getScalingFactor() + ".log";
        String sinkLogFileName = argumentClass.getOutputDirName() + "/sink-" + logFilePrefix;
        String spoutLogFileName = argumentClass.getOutputDirName() + "/spout-" + logFilePrefix;

        String taskPropFilename=argumentClass.getTasksPropertiesFilename();
        System.out.println("taskPropFilename-"+taskPropFilename);

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("spout", new SampleSpoutWithAcking(argumentClass.getInputDatasetPathName(), spoutLogFileName, argumentClass.getScalingFactor()), 1);

        //        builder.setSpout("spout", new OurRandomIntegerWithCHKPTSpout());
//        builder.setSpout("spout", new fooRandomIntegerWithCHKPTSpout());

        builder.setBolt("fooPartial2", new fooSleep("2"), 1).shuffleGrouping("spout");
        builder.setBolt("fooPartial3", new fooSleep("3"), 1).shuffleGrouping("fooPartial2");
        builder.setBolt("fooPartial4", new fooSleep("4"), 1).shuffleGrouping("fooPartial3");
        builder.setBolt("fooPartial5", new fooSleep("5"), 1).shuffleGrouping("fooPartial4");
        builder.setBolt("fooPartial6", new fooSleep("6"), 1).shuffleGrouping("fooPartial5");

        builder.setBolt("sink", new Sink(sinkLogFileName), 1).shuffleGrouping("fooPartial6");

        Config conf = new Config();
//        conf.setNumWorkers(5);
        conf.setNumAckers(1);
        conf.setDebug(false);
        conf.put(Config.TOPOLOGY_BACKPRESSURE_ENABLE,false);
        conf.put(Config.TOPOLOGY_DEBUG, false);
//        conf.put(Config.TOPOLOGY_STATE_CHECKPOINT_INTERVAL,30000);
//        conf.put(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS,30); // in sec.
        conf.put(Config.TOPOLOGY_STATE_PROVIDER,"org.apache.storm.redis.state.RedisKeyValueStateProvider");


//        conf.put(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS,10);

        System.out.println("TEST: inside main");

        if (argumentClass.getDeploymentMode().equals("C")) {
//            conf.setNumWorkers(1);
//            conf.setNumWorkers(6);
            StormSubmitter.submitTopology(argumentClass.getTopoName(), conf, builder.createTopology());
        }

        else {
            LocalCluster cluster = new LocalCluster();
            StormTopology topology = builder.createTopology();
            cluster.submitTopology("test", conf, topology);
            Utils.sleep(400000000);
            cluster.killTopology("test");
            cluster.shutdown();
        }
    }
}


//    L   IdentityTopology   /Users/anshushukla/Downloads/Incomplete/stream/PStormScheduler/src/test/java/operation/output/eventDist.csv     PLUG-210  1.0   /Users/anshushukla/data/output/temp      /Users/anshushukla/Downloads/Incomplete/stream/iot-bm-For-Scheduler/modules/tasks/src/main/resources/tasks.properties  test