package ustc.nodb.game;

import ustc.nodb.cluster.*;
import ustc.nodb.properties.*;
import java.util.*;

public class ClusterPackEdgeGame implements GameStrategy
{
    private final HashMap<Integer, Integer> clusterPartition;
    private final EdgeStreamCluster edgeStreamCluster;
    private final HashSet<Integer> clusterList;
    private final double[] partitionLoad;
    private double lambda;
    
    public ClusterPackEdgeGame(final EdgeStreamCluster edgeStreamCluster, final HashSet<Integer> clusterList, final int batchID) {
        this.edgeStreamCluster = edgeStreamCluster;
        this.clusterList = clusterList;
        this.clusterPartition = new HashMap<Integer, Integer>();
        this.partitionLoad = new double[GlobalConfig.getPartitionNum()];
        this.lambda = edgeStreamCluster.getBatchVertex(batchID) * Math.pow(GlobalConfig.getPartitionNum(), 3.0) / Math.pow(edgeStreamCluster.getBatchEdge(batchID), 2.0);
    }
    
    @Override
    public void initGame() {
        int partition = 0;
        for (final Integer clusterId : this.clusterList) {
            double minLoad = GlobalConfig.getECount();
            for (int i = 0; i < GlobalConfig.getPartitionNum(); ++i) {
                if (this.partitionLoad[i] < minLoad) {
                    minLoad = this.partitionLoad[i];
                    partition = i;
                }
            }
            this.clusterPartition.put(clusterId, partition);
            final double[] partitionLoad = this.partitionLoad;
            final int n = partition;
            partitionLoad[n] += this.edgeStreamCluster.getVolume(clusterId);
        }
    }
    
    @Override
    public double computeCost(final int clusterId, final int partition) {
        double loadPart = 0.0;
        double repPart = this.edgeStreamCluster.getClusterVertex(clusterId);
        final int oldPartition = this.clusterPartition.get(clusterId);
        loadPart = this.partitionLoad[oldPartition];
        if (partition != oldPartition) {
            loadPart = this.partitionLoad[partition] + this.edgeStreamCluster.getVolume(clusterId);
        }
        for (final Integer repVertex : this.edgeStreamCluster.getRepVertexEachCluster(clusterId)) {
            int repCnt = 0;
            for (final Integer neighbour : this.edgeStreamCluster.getRepVertexBelongsTo(repVertex)) {
                if (!this.clusterList.contains(neighbour)) {
                    continue;
                }
                if (this.clusterPartition.get(neighbour) != partition) {
                    continue;
                }
                ++repCnt;
            }
            repPart += 1.0 / repCnt;
        }
        final double k = GlobalConfig.getPartitionNum();
        return this.lambda / k * loadPart * this.edgeStreamCluster.getVolume(clusterId) + repPart;
    }
    
    @Override
    public void startGame() {
        boolean finish = false;
        while (!finish) {
            finish = true;
            for (final Integer clusterId : this.clusterList) {
                double minCost = Double.MAX_VALUE;
                int minPartition = this.clusterPartition.get(clusterId);
                for (int j = 0; j < GlobalConfig.getPartitionNum(); ++j) {
                    final double cost = this.computeCost(clusterId, j);
                    if (cost <= minCost) {
                        minCost = cost;
                        minPartition = j;
                    }
                }
                if (minPartition != this.clusterPartition.get(clusterId)) {
                    finish = false;
                    final double[] partitionLoad = this.partitionLoad;
                    final int n = minPartition;
                    partitionLoad[n] += this.edgeStreamCluster.getVolume(clusterId);
                    final double[] partitionLoad2 = this.partitionLoad;
                    final int intValue = this.clusterPartition.get(clusterId);
                    partitionLoad2[intValue] -= this.edgeStreamCluster.getVolume(clusterId);
                    this.clusterPartition.put(clusterId, minPartition);
                }
            }
        }
    }
    
    public HashMap<Integer, Integer> getClusterPartition() {
        return this.clusterPartition;
    }
}
