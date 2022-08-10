package ustc.nodb.main;
import ustc.nodb.Graph.Graph;
import ustc.nodb.Graph.OriginGraph;
import ustc.nodb.cluster.Holl;
import ustc.nodb.cluster.StreamCluster;
import ustc.nodb.game.ClusterPackGame;
import ustc.nodb.partitioner.CluSP;
import ustc.nodb.partitioner.TwoPS;
import ustc.nodb.properties.GlobalConfig;
import ustc.nodb.thread.ClusterGameTask;

import java.util.*;

import java.io.*;
import java.util.concurrent.*;

public class TwoPSMain {


    public static void main(String[] args) {

        if (args.length < 9) {
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

        Graph graph = new OriginGraph();

        System.out.println("input graph: " + GlobalConfig.inputGraphPath);

        System.out.println("---------------start-------------");

        long beforeUsedMem = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();

        long startTime = System.currentTimeMillis();

        Holl holl = new Holl(graph);
        holl.preStreamCluster();
        holl.startSteamCluster();

        TwoPS twoPS = new TwoPS(holl, graph);
        twoPS.clusterPartition();
        twoPS.prePartition();
        twoPS.remainPartition();

        long endTime = System.currentTimeMillis();
        double rf = twoPS.getReplicateFactor();
        double lb = twoPS.getLoadBalance();

        // free unused mem
        graph.clear();
        System.gc();

        long afterUsedMem = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
        long memoryUsed = (afterUsedMem - beforeUsedMem) >> 20;

        System.out.println("partition num:" + GlobalConfig.getPartitionNum());
        System.out.println("partition time: " + (endTime - startTime) + " ms");
        System.out.println("relative balance load:" + lb);
        System.out.println("replicate factor: " + rf);
        System.out.println("memory cost: " + memoryUsed + " MB");

        System.out.println("---------------end-------------");


    }
}