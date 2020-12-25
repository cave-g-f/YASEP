package ustc.nodb.main;

import ustc.nodb.Graph.Graph;
import ustc.nodb.Graph.OriginGraph;
import ustc.nodb.cluster.StreamCluster;
import ustc.nodb.game.ClusterPackGame;
import ustc.nodb.partitioner.CluSP;
import ustc.nodb.properties.GlobalConfig;
import ustc.nodb.thread.ClusterGameTask;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.*;

public class CluSPMain {

    static Graph graph = new OriginGraph();
    static HashMap<Integer, Integer> clusterPartition = new HashMap<>();
    static int roundCnt;

    public static void main(String[] args) throws IOException {

        long startTime = System.currentTimeMillis();
        graph.readGraphFromFile();

        StreamCluster streamCluster = new StreamCluster(graph);
        streamCluster.startSteamCluster();

        ArrayList<Integer> clusterList = streamCluster.getClusterList();

        // parallel game theory
        ExecutorService taskPool = Executors.newFixedThreadPool(GlobalConfig.getThreads());
        CompletionService<ClusterPackGame> completionService = new ExecutorCompletionService<>(taskPool);

        int clusterSize = clusterList.size();

        System.out.println("cluster size: " + clusterSize);

        int taskNum = (clusterSize + GlobalConfig.getBatchSize() - 1) / GlobalConfig.getBatchSize();

        System.out.println("taskNum: " + taskNum);
        System.out.println("cluster num: " + clusterSize);

        long gameStartTime = System.currentTimeMillis();

        for(int i = 0; i < taskNum; i++){
            completionService.submit(new ClusterGameTask(streamCluster, i));
        }

        for(int i = 0; i < taskNum; i++){
            try{
                Future<ClusterPackGame> result = completionService.take();
                ClusterPackGame game = result.get();
                clusterPartition.putAll(game.getClusterPartition());
                roundCnt += game.getRoundCnt();
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }
        }

        taskPool.shutdownNow();

        long gameEndTime = System.currentTimeMillis();

        System.out.println(clusterPartition.size());

        // start streaming partition
        CluSP cluSP = new CluSP(graph, streamCluster, clusterPartition);
        cluSP.performStep();
        long endTime = System.currentTimeMillis();
        double rf = cluSP.getReplicateFactor();
        double lb = cluSP.getLoadBalance();

        cluSP.output();

        // free unused mem
        graph.clear();
        cluSP.clear();
        System.gc();

        long memoryUsed = Runtime.getRuntime().totalMemory() >> 20;

        System.out.println("partition time: " + (endTime - startTime) + " ms");
        System.out.println("relative balance load:" + lb);
        System.out.println("replicate factor: " + rf);
        System.out.println("memory cost: " + memoryUsed + " MB");
        System.out.println("total game round:" + roundCnt);
        System.out.println("cluster game time: " + (gameEndTime - gameStartTime) + " ms");
    }
}
