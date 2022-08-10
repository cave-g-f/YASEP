package ustc.nodb.main;

import ustc.nodb.Graph.Graph;
import ustc.nodb.Graph.OriginGraph;
import ustc.nodb.cluster.StreamCluster;
import ustc.nodb.game.ClusterPackGame;
import ustc.nodb.partitioner.CluSP;
import ustc.nodb.properties.GlobalConfig;
import ustc.nodb.thread.ClusterGameTask;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.*;

public class CluSPMain {


    public static void main(String[] args) throws IOException {

        if(args.length < 8)
        {
            System.out.println("Usage: [vCount] [eCount] [input_path] [threads] [partition_number]" +
                    " [batch_size] [output_path] [alpha]");
        }

        GlobalConfig.vCount = Integer.parseInt(args[0]);
        GlobalConfig.eCount = Integer.parseInt(args[1]);
        GlobalConfig.inputGraphPath = args[2];
        GlobalConfig.threads = Integer.parseInt(args[3]);
        GlobalConfig.partitionNum = Integer.parseInt(args[4]);
        GlobalConfig.batchSize = Integer.parseInt(args[5]);
        GlobalConfig.outputGraphPath = args[6];
        GlobalConfig.alpha =  Float.parseFloat(args[7]);

        Graph graph = new OriginGraph();
        HashMap<Integer, Integer> clusterPartition = new HashMap<>();
        int roundCnt = 0;

        System.out.println("input graph: " + GlobalConfig.inputGraphPath);

        System.out.println("---------------start-------------");

        long beforeUsedMem = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();

        long startTime = System.currentTimeMillis();

        StreamCluster streamCluster = new StreamCluster(graph);
        streamCluster.startSteamCluster();

        List<Integer> clusterList = streamCluster.getClusterList();

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

        // free unused mem
        graph.clear();
        cluSP.clear();
        System.gc();

        long afterUsedMem = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
        long memoryUsed = (afterUsedMem - beforeUsedMem) >> 20;

        System.out.println("partition num:" + GlobalConfig.getPartitionNum());
        System.out.println("partition time: " + (endTime - startTime) + " ms");
        System.out.println("relative balance load:" + lb);
        System.out.println("replicate factor: " + rf);
        System.out.println("memory cost: " + memoryUsed + " MB");
        System.out.println("total game round:" + roundCnt);
        System.out.println("cluster game time: " + (gameEndTime - gameStartTime) + " ms");

        System.out.println("---------------end-------------");
    }
}
