package ustc.nodb.partitioner;

import ustc.nodb.Graph.*;
import ustc.nodb.cluster.*;
import ustc.nodb.IO.*;
import ustc.nodb.properties.*;
import ustc.nodb.core.*;
import java.util.*;

public class CLUGPEdge implements PartitionStrategy
{
    private final int[] partitionLoad;
    private final HashMap<Integer, HashSet<Integer>> replicateTable;
    private final HashMap<Integer, Integer> clusterPartition;
    private final BatchGraph batchGraph;
    private final EdgeStreamCluster edgeStreamCluster;
    private int maxLoad;
    private int batchId;
    
    public CLUGPEdge(final EdgeStreamCluster edgeStreamCluster, final HashMap<Integer, Integer> clusterPartition, final BatchFileIO batchFileIO, final int batchId) {
        this.clusterPartition = clusterPartition;
        this.edgeStreamCluster = edgeStreamCluster;
        this.partitionLoad = new int[GlobalConfig.getPartitionNum()];
        this.replicateTable = new HashMap<Integer, HashSet<Integer>>();
        this.batchGraph = new BatchGraph(batchFileIO, batchId);
        this.maxLoad = (int)((edgeStreamCluster.getBatchEdge(batchId) / GlobalConfig.getPartitionNum() + 1) * GlobalConfig.tau);
        this.batchId = batchId;
    }
    
    @Override
    public void performStep() {
        Edge edge;
        while ((edge = this.batchGraph.readStep()) != null) {
            final int src = edge.getSrcVId();
            final int dest = edge.getDestVId();
            final int cluster = edge.getClusterId();
            if (!this.clusterPartition.containsKey(cluster)) {
                System.out.println("cluster id: " + cluster + " batch: " + this.batchId);
            }
            int edgePartition = this.clusterPartition.get(edge.getClusterId());
            if (!this.replicateTable.containsKey(src)) {
                this.replicateTable.put(src, new HashSet<Integer>());
            }
            if (!this.replicateTable.containsKey(dest)) {
                this.replicateTable.put(dest, new HashSet<Integer>());
            }
            if (this.partitionLoad[edgePartition] >= this.maxLoad) {
                for (int i = 0; i < GlobalConfig.partitionNum; ++i) {
                    if (this.partitionLoad[i] < this.maxLoad) {
                        edgePartition = i;
                        break;
                    }
                }
            }
            final int[] partitionLoad = this.partitionLoad;
            final int n = edgePartition;
            ++partitionLoad[n];
            this.replicateTable.get(src).add(edgePartition);
            this.replicateTable.get(dest).add(edgePartition);
        }
    }
    
    @Override
    public void clear() {
        this.batchGraph.clear();
        this.replicateTable.clear();
    }
    
    @Override
    public double getReplicateFactor() {
        double sum = 0.0;
        for (final Integer integer : this.replicateTable.keySet()) {
            sum += this.replicateTable.get(integer).size();
        }
        return sum / this.replicateTable.size();
    }
    
    @Override
    public double getLoadBalance() {
        double maxLoad = 0.0;
        for (int i = 0; i < GlobalConfig.getPartitionNum(); ++i) {
            if (maxLoad < this.partitionLoad[i]) {
                maxLoad = this.partitionLoad[i];
            }
        }
        return GlobalConfig.getPartitionNum() / (double)GlobalConfig.getECount() * maxLoad;
    }
    
    public HashMap<Integer, HashSet<Integer>> getReplicateTable() {
        return this.replicateTable;
    }
    
    public int[] getPartitionLoad() {
        return this.partitionLoad;
    }
}
