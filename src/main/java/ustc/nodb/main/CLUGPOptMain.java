package ustc.nodb.main;

import ustc.nodb.Graph.Graph;
import ustc.nodb.Graph.OriginGraph;
import ustc.nodb.IO.BatchFileIO;
import ustc.nodb.cluster.EdgeStreamCluster;
import ustc.nodb.partitioner.CLUGPEdge;
import ustc.nodb.properties.GlobalConfig;
import ustc.nodb.thread.CLUGPEdgeTask;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

public class CLUGPOptMain {

    public static void main(String[] args) throws IOException {

        if(args.length < 9)
        {
            System.out.println("Usage: [vCount] [eCount] [input_path] [threads] [partition_number]" +
                    " [batch_size] [output_path] [tau] [batchEdgePath]");
        }

        GlobalConfig.vCount = Integer.parseInt(args[0]);
        GlobalConfig.eCount = Integer.parseInt(args[1]);
        GlobalConfig.inputGraphPath = args[2];
        GlobalConfig.threads = Integer.parseInt(args[3]);
        GlobalConfig.partitionNum = Integer.parseInt(args[4]);
        GlobalConfig.batchSize = Integer.parseInt(args[5]);
        GlobalConfig.outputGraphPath = args[6];
        GlobalConfig.tau = Double.parseDouble(args[7]);
        GlobalConfig.batchEdgePath = args[8];

        System.out.println("input graph: " + GlobalConfig.inputGraphPath);
        System.out.println("partition number " + GlobalConfig.partitionNum);
        System.out.println("---------------start-------------");

        Graph graph = new OriginGraph();
        BatchFileIO batchFileIO = new BatchFileIO();
        HashMap<Integer, HashSet<Integer>> replicateTable = new HashMap<>();
        int[] partitionLoad = new int[GlobalConfig.getPartitionNum()];
        int roundCnt = 0;

        long beforeUsedMem = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();

        System.out.println("---------------start edge clustering-------------");
        long startTime = System.currentTimeMillis();
        EdgeStreamCluster edgeStreamCluster = new EdgeStreamCluster(graph, batchFileIO);
        edgeStreamCluster.startSteamCluster();
        long clusterEndTime = System.currentTimeMillis();
        System.out.println("cluster time: " + (clusterEndTime - startTime) + "ms");


        // parallel game theory
        System.out.println("---------------start game + refine-------------");
        ExecutorService taskPool = Executors.newFixedThreadPool(GlobalConfig.getThreads());
        CompletionService<CLUGPEdge> completionService = new ExecutorCompletionService<>(taskPool);

        int taskNum = edgeStreamCluster.getBatchNum();
        System.out.println("task number: " + taskNum);
        long gameStartTime = System.currentTimeMillis();

        for(int i = 0; i < taskNum; i++){
            completionService.submit(new CLUGPEdgeTask(edgeStreamCluster, batchFileIO, i));
        }

        for(int i = 0; i < taskNum; i++){
            try{
                Future<CLUGPEdge> result = completionService.take();
                CLUGPEdge edge = result.get();
                for(Map.Entry<Integer, HashSet<Integer>> entry : edge.getReplicateTable().entrySet()){
                    if(!replicateTable.containsKey(entry.getKey())){
                        replicateTable.put(entry.getKey(), entry.getValue());
                    }else{
                        replicateTable.get(entry.getKey()).addAll(entry.getValue());
                    }
                }
                int[] subPartitionLoad = edge.getPartitionLoad();
                for(int j = 0; j < GlobalConfig.getPartitionNum(); j++){
                    partitionLoad[j] += subPartitionLoad[j];
                }
                edge.clear();
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }
        }

        taskPool.shutdownNow();

        long gameEndTime = System.currentTimeMillis();

        // calculate replication factor
        double rf = 0.0;
        for (Integer integer : replicateTable.keySet()) {
            rf += replicateTable.get(integer).size();
        }
        rf  =  rf / replicateTable.size();
        System.out.println(replicateTable.size());

        int totalE = 0;
        for(int i = 0 ; i < edgeStreamCluster.getBatchNum(); i++)
        {
            totalE += edgeStreamCluster.getBatchEdge(i);
        }
        System.out.println(totalE);

        // calculate relative load balance
        double lb = 0.0;
        double maxLoad = 0.0;
        for (int i = 0; i < GlobalConfig.getPartitionNum(); i++) {
            if (maxLoad < partitionLoad[i]) {
                maxLoad = partitionLoad[i];
            }
        }
        lb =  (double) GlobalConfig.getPartitionNum() / GlobalConfig.getECount() * maxLoad;



        // free unused mem
        graph.clear();
        System.gc();

        long afterUsedMem = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
        long memoryUsed = (afterUsedMem - beforeUsedMem) >> 20;

        System.out.println("partition num:" + GlobalConfig.getPartitionNum());
        System.out.println("partition time: " + (gameEndTime - startTime) + " ms");
        System.out.println("relative balance load:" + lb);
        System.out.println("replicate factor: " + rf);
        System.out.println("memory cost: " + memoryUsed + " MB");
        System.out.println("total game round:" + roundCnt);
        System.out.println("cluster game time: " + (gameEndTime - gameStartTime) + " ms");

        System.out.println("---------------end-------------");
    }
}
