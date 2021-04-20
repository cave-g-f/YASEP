package ustc.nodb.partitioner;

import ustc.nodb.Graph.Graph;
import ustc.nodb.cluster.StreamCluster;
import ustc.nodb.core.Edge;
import ustc.nodb.properties.GlobalConfig;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

public class CluSP implements PartitionStrategy {

    private final Graph originGraph;
    private final StreamCluster streamCluster;
    private final int[] partitionLoad;
    private final HashMap<Integer, HashSet<Integer>> replicateTable;
    private final HashMap<Integer, Integer> clusterPartition;
    private BufferedWriter bufferedWriter;

    public CluSP(Graph originGraph, StreamCluster streamCluster, HashMap<Integer, Integer> clusterPartition) {
        this.originGraph = originGraph;
        this.streamCluster = streamCluster;
        this.clusterPartition = clusterPartition;
        partitionLoad = new int[GlobalConfig.getPartitionNum()];
        replicateTable = new HashMap<>(GlobalConfig.vCount);

//        try {
//            File file = new File(GlobalConfig.getOutputGraphPath() + ".txt");
//            if (!file.exists()) file.createNewFile();
//
//            FileOutputStream outputStream = new FileOutputStream(file);
//            bufferedWriter = new BufferedWriter(new OutputStreamWriter(outputStream));
//        } catch (Exception ex) {
//
//        }
    }

    @Override
    public void performStep() {

        double maxLoad = (double) GlobalConfig.eCount / GlobalConfig.partitionNum * 1.1;

        originGraph.readGraphFromFile();
        Edge edge;
        while ((edge = originGraph.readStep()) != null) {
            int src = edge.getSrcVId();
            int dest = edge.getDestVId();
            int srcPartition = clusterPartition.get(streamCluster.getClusterId(src));
            int destPartition = clusterPartition.get(streamCluster.getClusterId(dest));
            int edgePartition = -1;

            if (!replicateTable.containsKey(src)) replicateTable.put(src, new HashSet<>());
            if (!replicateTable.containsKey(dest)) replicateTable.put(dest, new HashSet<>());
            if (partitionLoad[srcPartition] > maxLoad && partitionLoad[destPartition] > maxLoad) {
                for (int i = 0; i < GlobalConfig.partitionNum; i++) {
                    if (partitionLoad[i] <= maxLoad) {
                        edgePartition = i;
                        srcPartition = i;
                        destPartition = i;
                        break;
                    }
                }
            } else if (partitionLoad[srcPartition] > partitionLoad[destPartition]) {
                edgePartition = destPartition;
                srcPartition = destPartition;
            } else {
                edgePartition = srcPartition;
                destPartition = srcPartition;
            }

            partitionLoad[edgePartition]++;

//            output(edge, edgePartition);

//            try {
//                bufferedWriter.flush();
//            } catch (IOException e) {
//                e.printStackTrace();
//            }

            replicateTable.get(src).add(srcPartition);
            replicateTable.get(dest).add(destPartition);
        }
    }

    @Override
    public void clear() {
        replicateTable.clear();
    }

    @Override
    public double getReplicateFactor() {
        double sum = 0.0;
        for (Integer integer : replicateTable.keySet()) {
            sum += replicateTable.get(integer).size();
        }
        return sum / GlobalConfig.getVCount();
    }

    @Override
    public double getLoadBalance() {
        double maxLoad = 0.0;
        for (int i = 0; i < GlobalConfig.getPartitionNum(); i++) {
            if (maxLoad < partitionLoad[i]) {
                maxLoad = partitionLoad[i];
            }
        }
        return (double) GlobalConfig.getPartitionNum() / GlobalConfig.getECount() * maxLoad;
    }

    public void output(Edge edge, int partition) {
        try {
            int src = edge.getSrcVId();
            int dest = edge.getDestVId();
            bufferedWriter.write(Integer.toString(src) + "\t" + Integer.toString(dest) + "\t" + Integer.toString(partition));
            bufferedWriter.write("\n");
        } catch (Exception ex) {

        }
    }

}
