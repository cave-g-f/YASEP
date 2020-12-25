package ustc.nodb.partitioner;

import ustc.nodb.Graph.Graph;
import ustc.nodb.cluster.StreamCluster;
import ustc.nodb.core.Edge;
import ustc.nodb.Graph.OriginGraph;
import ustc.nodb.game.ClusterPackGame;
import ustc.nodb.properties.GlobalConfig;
import ustc.nodb.Graph.SketchGraph;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

public class CluSP implements PartitionStrategy {

    private final Graph originGraph;
    private final StreamCluster streamCluster;
    private final ArrayList<HashSet<Edge>> partitionTable;
    private final HashMap<Integer, HashSet<Integer>> replicateTable;
    private final HashMap<Integer, Integer> clusterPartition;

    public CluSP(Graph originGraph, StreamCluster streamCluster, HashMap<Integer, Integer> clusterPartition) {
        this.originGraph = originGraph;
        this.streamCluster = streamCluster;
        this.clusterPartition = clusterPartition;
        partitionTable = new ArrayList<>();
        for (int i = 0; i < GlobalConfig.getPartitionNum(); i++)
            partitionTable.add(new HashSet<>());
        replicateTable = new HashMap<>();
    }

    @Override
    public void performStep() {

        for (Edge edge : originGraph.getEdgeList()) {
            int src = edge.getSrcVId();
            int dest = edge.getDestVId();
            int srcPartition = clusterPartition.get(streamCluster.getClusterId(src));
            int destPartition = clusterPartition.get(streamCluster.getClusterId(dest));
            int edgePartition = -1;

            if (!replicateTable.containsKey(src)) replicateTable.put(src, new HashSet<>());
            if (!replicateTable.containsKey(dest)) replicateTable.put(dest, new HashSet<>());
            if (srcPartition == destPartition)
                edgePartition = srcPartition;
            else {
                if (partitionTable.get(srcPartition).size() > partitionTable.get(destPartition).size()) {
                    edgePartition = destPartition;
                    srcPartition = destPartition;
                } else {
                    edgePartition = srcPartition;
                    destPartition = srcPartition;
                }
            }

            partitionTable.get(edgePartition).add(edge);
            replicateTable.get(src).add(srcPartition);
            replicateTable.get(dest).add(destPartition);
        }
    }

    @Override
    public void clear(){
        partitionTable.clear();
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
            if(maxLoad < partitionTable.get(i).size()){
                maxLoad = partitionTable.get(i).size();
            }
        }
        return (double)GlobalConfig.getPartitionNum() / GlobalConfig.getECount() * maxLoad;
    }

    @Override
    public void output() throws IOException {

        for(int i = 0 ; i < GlobalConfig.getPartitionNum(); i++)
        {
            File file = new File(GlobalConfig.getOutputGraphPath() + "_" + i + ".txt");
            if(!file.exists()) file.createNewFile();

            FileOutputStream outputStream = new FileOutputStream(file);
            BufferedWriter bufferedWriter = new BufferedWriter(new OutputStreamWriter(outputStream));

//            System.out.println(partitionTable.get(i).size());
            for(Edge edge : partitionTable.get(i)){
                int src = edge.getSrcVId();
                int dest = edge.getDestVId();
                bufferedWriter.write(Integer.toString(src) + "\t" + Integer.toString(dest));
                bufferedWriter.write("\n");
            }
            bufferedWriter.flush();
            bufferedWriter.close();
        }
    }

}
