package ustc.nodb.game;

import ustc.nodb.cluster.Cluster;
import ustc.nodb.cluster.EdgeStreamCluster;
import ustc.nodb.properties.GlobalConfig;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

public class ClusterPackEdgeGame implements GameStrategy {

    private final HashMap<Integer, Integer> clusterPartition; // key: cluster value: partition
    private final EdgeStreamCluster edgeStreamCluster;
    private final ArrayList<Cluster> clusterList;
    private final double[] partitionLoad;
    private double lambda;

    public ClusterPackEdgeGame(EdgeStreamCluster edgeStreamCluster, ArrayList<Cluster> clusterList, int batchID) {
        this.edgeStreamCluster = edgeStreamCluster;
        this.clusterList = clusterList;
        this.clusterPartition = new HashMap<>();
        this.partitionLoad = new double[GlobalConfig.getPartitionNum()];

        this.lambda = edgeStreamCluster.getBatchVertex(batchID) * Math.pow(GlobalConfig.getPartitionNum(), 3) /
                Math.pow(edgeStreamCluster.getBatchEdge(batchID), 2);
    }

    @Override
    public void initGame() {
        int partition = 0;
        for (Cluster c : clusterList) {
            double minLoad = GlobalConfig.getECount();
            for (int i = 0; i < GlobalConfig.getPartitionNum(); i++) {
                if (partitionLoad[i] < minLoad) {
                    minLoad = partitionLoad[i];
                    partition = i;
                }
            }
            clusterPartition.put(c, partition);
            partitionLoad[partition] += edgeStreamCluster.getVolume(clusterId);
        }
    }

    @Override
    public double computeCost(int clusterId, int partition) {

        double loadPart = 0.0;
        double repPart = 0.0;
        int oldPartition = clusterPartition.get(clusterId);

        loadPart = partitionLoad[oldPartition];

        if (partition != oldPartition)
            loadPart = partitionLoad[partition] + edgeStreamCluster.getVolume(clusterId);

        for (Integer repVertex : edgeStreamCluster.getRepVertexEachCluster(clusterId)){
            int repCnt = 0;
            for (Integer neighbour : edgeStreamCluster.getRepVertexBelongsTo(repVertex)){
                if(!clusterList.contains(neighbour)) continue;
                if(clusterPartition.get(neighbour) == partition){
                    repCnt++;
                }
            }
            repPart += 1 / (double)repCnt;
        }

        double k = GlobalConfig.getPartitionNum();
        return lambda / k * loadPart * edgeStreamCluster.getVolume(clusterId) + repPart;
    }

    @Override
    public void startGame() {
        boolean finish = false;

        while (!finish) {
            finish = true;
            for (Integer clusterId : clusterList) {
                double minCost = Double.MAX_VALUE;
                int minPartition = clusterPartition.get(clusterId);

                for (int j = 0; j < GlobalConfig.getPartitionNum(); j++) {
                    double cost = computeCost(clusterId, j);
                    if (cost <= minCost) {
                        minCost = cost;
                        minPartition = j;
                    }
                }

                if (minPartition != clusterPartition.get(clusterId)) {
                    finish = false;

                    // update partition load
                    partitionLoad[minPartition] += edgeStreamCluster.getVolume(clusterId);
                    partitionLoad[clusterPartition.get(clusterId)] -= edgeStreamCluster.getVolume(clusterId);
                    clusterPartition.put(clusterId, minPartition);
                }
            }
        }
    }

    public HashMap<Integer, Integer> getClusterPartition() {
        return clusterPartition;
    }
}
