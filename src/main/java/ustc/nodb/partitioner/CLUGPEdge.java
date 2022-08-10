package ustc.nodb.partitioner;

import ustc.nodb.Graph.BatchGraph;
import ustc.nodb.IO.BatchFileIO;
import ustc.nodb.cluster.EdgeStreamCluster;
import ustc.nodb.core.Edge;
import ustc.nodb.properties.GlobalConfig;

import java.util.HashMap;
import java.util.HashSet;

public class CLUGPEdge implements PartitionStrategy {

    private final int[] partitionLoad;
    private final HashMap<Integer, HashSet<Integer>> replicateTable;
    private final HashMap<Integer, Integer> clusterPartition;
    private final BatchGraph batchGraph;
    private final EdgeStreamCluster edgeStreamCluster;
    private int maxLoad;
    private int batchId;

    public CLUGPEdge(EdgeStreamCluster edgeStreamCluster, HashMap<Integer, Integer> clusterPartition,
                     BatchFileIO batchFileIO, int batchId) {
        this.clusterPartition = clusterPartition;
        this.edgeStreamCluster = edgeStreamCluster;
        this.partitionLoad = new int[GlobalConfig.getPartitionNum()];
        this.replicateTable = new HashMap<>();
        this.batchGraph = new BatchGraph(batchFileIO, batchId);
        this.maxLoad = (int) ((edgeStreamCluster.getBatchEdge(batchId) / GlobalConfig.getPartitionNum() + 1) * GlobalConfig.tau);
        this.batchId = batchId;
    }

    @Override
    public void performStep() {
        Edge edge;
        while((edge = batchGraph.readStep()) != null){
            int src = edge.getSrcVId();
            int dest = edge.getDestVId();
            int cluster = edge.getClusterId();
            if(!clusterPartition.containsKey(cluster)){
                System.out.println("cluster id: " + cluster + " batch: " + batchId);
            }
            int edgePartition = clusterPartition.get(edge.getClusterId());
            if (!replicateTable.containsKey(src)) replicateTable.put(src, new HashSet<>());
            if (!replicateTable.containsKey(dest)) replicateTable.put(dest, new HashSet<>());
            if (partitionLoad[edgePartition] >= maxLoad) {
                for (int i = 0; i < GlobalConfig.partitionNum; i++) {
                    if (partitionLoad[i] < maxLoad) {
                        edgePartition = i;
                        break;
                    }
                }
            }
            partitionLoad[edgePartition]++;
            replicateTable.get(src).add(edgePartition);
            replicateTable.get(dest).add(edgePartition);
        }
    }

    @Override
    public void clear() {
        batchGraph.clear();
        replicateTable.clear();
    }

    @Override
    public double getReplicateFactor() {
        double sum = 0.0;
        for (Integer integer : replicateTable.keySet()) {
            sum += replicateTable.get(integer).size();
        }
        return sum / replicateTable.size();
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

    public HashMap<Integer, HashSet<Integer>> getReplicateTable(){
        return replicateTable;
    }

    public int[] getPartitionLoad(){
        return partitionLoad;
    }
}
