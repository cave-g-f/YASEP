package ustc.nodb.main;

import ustc.nodb.Graph.Graph;
import ustc.nodb.Graph.OriginGraph;
import ustc.nodb.cluster.StreamCluster;
import ustc.nodb.core.Edge;
import ustc.nodb.partitioner.CluSP;
import ustc.nodb.partitioner.Mint;
import ustc.nodb.properties.GlobalConfig;
import ustc.nodb.thread.ClusterGameTask;
import ustc.nodb.thread.EdgeGameTask;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.*;

public class MintMain {

    static Graph graph = new OriginGraph();
    static HashMap<Edge, Integer> edgePartition = new HashMap<>();

    public static void main(String[] args){

        long startTime = System.currentTimeMillis();
        graph.readGraphFromFile();

        int edgeSize = graph.getECount();

        // parallel game theory
        ExecutorService taskPool = Executors.newFixedThreadPool(GlobalConfig.getThreads());
        CompletionService<HashMap<Edge, Integer>> completionService = new ExecutorCompletionService<>(taskPool);

        int taskNum = (edgeSize + GlobalConfig.getBatchSize() - 1) / GlobalConfig.getBatchSize();

        System.out.println("taskNum: " + taskNum);

        for(int i = 0; i < taskNum; i++){
            completionService.submit(new EdgeGameTask(graph, i));
        }

        for(int i = 0; i < taskNum; i++){
            try{
                Future<HashMap<Edge, Integer>> result = completionService.take();
                edgePartition.putAll(result.get());
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }
        }

        taskPool.shutdownNow();

        long endTime = System.currentTimeMillis();

        // start streaming partition
        Mint mint = new Mint(edgePartition);
        mint.performStep();

        System.out.println("partition time: " + (endTime - startTime) + " ms");
        System.out.println("relative balance load:" + mint.getLoadBalance());
        System.out.println("replicate factor: " + mint.getReplicateFactor());

        // free unused mem
        graph.clear();
        mint.clear();
        System.gc();
        long memoryUsed = Runtime.getRuntime().totalMemory() >> 20;

        System.out.println("memory cost: " + memoryUsed + " MB");
    }
}
