package ustc.nodb.main;

import ustc.nodb.properties.*;
import ustc.nodb.IO.*;
import ustc.nodb.cluster.*;
import ustc.nodb.partitioner.*;
import ustc.nodb.thread.*;
import ustc.nodb.Graph.*;
import java.util.concurrent.*;
import java.util.*;
import java.io.*;

public class CLUGPOptMain
{
    public static void main(final String[] args) throws IOException {
        if (args.length < 9) {
            System.out.println("Usage: [vCount] [eCount] [input_path] [threads] [partition_number] [batch_size] [output_path] [tau] [batchEdgePath]");
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
        final Graph graph = new OriginGraph();
        final BatchFileIO batchFileIO = new BatchFileIO();
        final HashMap<Integer, HashSet<Integer>> replicateTable = new HashMap<Integer, HashSet<Integer>>();
        final int[] partitionLoad = new int[GlobalConfig.getPartitionNum()];
        final int roundCnt = 0;
        final long beforeUsedMem = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
        System.out.println("---------------start edge clustering-------------");
        final long startTime = System.currentTimeMillis();
        final EdgeStreamCluster edgeStreamCluster = new EdgeStreamCluster(graph, batchFileIO);
        edgeStreamCluster.startSteamCluster();
        final long clusterEndTime = System.currentTimeMillis();
        System.out.println("cluster time: " + (clusterEndTime - startTime) + "ms");
        System.out.println("---------------start game + refine-------------");
        final ExecutorService taskPool = Executors.newFixedThreadPool(GlobalConfig.getThreads());
        final CompletionService<CLUGPEdge> completionService = new ExecutorCompletionService<CLUGPEdge>(taskPool);
        final int taskNum = edgeStreamCluster.getBatchNum();
        System.out.println("task number: " + taskNum);
        final long gameStartTime = System.currentTimeMillis();
        for (int i = 0; i < taskNum; ++i) {
            completionService.submit((Callable<CLUGPEdge>)new CLUGPEdgeTask(edgeStreamCluster, batchFileIO, i));
        }
        for (int i = 0; i < taskNum; ++i) {
            try {
                final Future<CLUGPEdge> result = completionService.take();
                final CLUGPEdge edge = result.get();
                for (final Map.Entry<Integer, HashSet<Integer>> entry : edge.getReplicateTable().entrySet()) {
                    if (!replicateTable.containsKey(entry.getKey())) {
                        replicateTable.put(entry.getKey(), entry.getValue());
                    }
                    else {
                        replicateTable.get(entry.getKey()).addAll(entry.getValue());
                    }
                }
                final int[] subPartitionLoad = edge.getPartitionLoad();
                for (int j = 0; j < GlobalConfig.getPartitionNum(); ++j) {
                    final int[] array = partitionLoad;
                    final int n = j;
                    array[n] += subPartitionLoad[j];
                }
                edge.clear();
            }
            catch (InterruptedException | ExecutionException ex) {
                ex.printStackTrace();
            }
        }
        taskPool.shutdownNow();
        final long gameEndTime = System.currentTimeMillis();
        double rf = 0.0;
        for (final Integer integer : replicateTable.keySet()) {
            rf += replicateTable.get(integer).size();
        }
        rf /= replicateTable.size();
        System.out.println(replicateTable.size());
        int totalE = 0;
        for (int k = 0; k < edgeStreamCluster.getBatchNum(); ++k) {
            totalE += edgeStreamCluster.getBatchEdge(k);
        }
        System.out.println(totalE);
        double lb = 0.0;
        double maxLoad = 0.0;
        for (int l = 0; l < GlobalConfig.getPartitionNum(); ++l) {
            if (maxLoad < partitionLoad[l]) {
                maxLoad = partitionLoad[l];
            }
        }
        lb = GlobalConfig.getPartitionNum() / (double)GlobalConfig.getECount() * maxLoad;
        graph.clear();
        System.gc();
        final long afterUsedMem = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
        final long memoryUsed = afterUsedMem - beforeUsedMem >> 20;
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
