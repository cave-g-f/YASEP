package ustc.nodb.partitioner;

import org.junit.Test;
import ustc.nodb.Graph.Graph;
import ustc.nodb.Graph.OriginGraph;
import ustc.nodb.Graph.SketchGraph;
import ustc.nodb.cluster.StreamCluster;
import ustc.nodb.partitioner.TcmSp;
import ustc.nodb.properties.GlobalConfig;
import ustc.nodb.thread.GameTask;

import java.util.*;
import java.util.concurrent.*;

import static org.junit.Assert.*;

public class SCPTest {

    Graph graph;
    HashMap<Integer, Integer> clusterPartition = new HashMap<>();

    public SCPTest(){
        graph = new OriginGraph();
    }

    @Test
    public void Test(){
        graph.readGraphFromFile();

        long startTime = System.currentTimeMillis();
        StreamCluster streamCluster = new StreamCluster(graph);
        streamCluster.startSteamCluster();

        // parallel game theory
        ExecutorService taskPool = Executors.newFixedThreadPool(10);
        CompletionService<HashMap<Integer, Integer>> completionService = new ExecutorCompletionService<>(taskPool);

        int clusterSize = streamCluster.getClusterList().size();
        int taskNum = (clusterSize + GlobalConfig.getBatchSize() - 1) / GlobalConfig.getBatchSize();

        System.out.println("taskNum: " + taskNum);
        System.out.println("cluster num: " + clusterSize);

        for(int i = 0; i < taskNum; i++){
            completionService.submit(new GameTask(streamCluster, i));
        }

        for(int i = 0; i < taskNum; i++){
            try{
                Future<HashMap<Integer, Integer>> result = completionService.take();
                clusterPartition.putAll(result.get());
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }
        }

        System.out.println(clusterPartition.size());

        // start streaming partition
        TcmSp tcmSp = new TcmSp(graph, streamCluster, clusterPartition);
        tcmSp.performStep();

        long endTime = System.currentTimeMillis();

        System.out.println("partition time: " + (endTime - startTime) + " ms");

        System.out.println("relative balance load:" + tcmSp.getLoadBalance());
        System.out.println("replicate factor: " + tcmSp.getReplicateFactor());
    }

}