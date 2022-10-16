package ustc.nodb.game;

import ustc.nodb.cluster.StreamCluster;
import ustc.nodb.properties.GlobalConfig;

import java.util.*;

public class ClusterPackGame implements GameStrategy {

    private final HashMap<Integer, Integer> clusterPartition; // key: cluster value: partition
    private final HashMap<Integer, Integer> cutCostValue; // key: cluster value: cutCost
    private final HashMap<Integer, HashSet<Integer>> clusterNeighbours;
    private final double[] partitionLoad;
    private final List<Integer> clusterList;
    private final StreamCluster streamCluster;
    private double beta = 0.0;
    private int roundCnt;

    public ClusterPackGame(StreamCluster streamCluster, List<Integer> clusterList) {
        this.clusterPartition = new HashMap<>();
        this.streamCluster = streamCluster;
        this.clusterList = clusterList;
        cutCostValue = new HashMap<>();
        partitionLoad = new double[GlobalConfig.getPartitionNum()];
        clusterNeighbours = new HashMap<>();
    }

    @Override
    public void initGame() {
        int partition = 0;
        for (Integer clusterId : clusterList) {
            double minLoad = GlobalConfig.getECount();
            for (int i = 0; i < GlobalConfig.getPartitionNum(); i++) {
                if (partitionLoad[i] < minLoad) {
                    minLoad = partitionLoad[i];
                    partition = i;
                }
            }
            clusterPartition.put(clusterId, partition);
            partitionLoad[partition] += streamCluster.getEdgeNum(clusterId, clusterId);
        }

        double sizePart = 0.0, cutPart = 0.0;
        for (Integer cluster1 : clusterList) {
            sizePart += streamCluster.getEdgeNum(cluster1, cluster1);

            for (Integer cluster2 : clusterList) {
                int innerCut = 0;
                int outerCut = 0;
                if (!cluster1.equals(cluster2)) {
                    innerCut = streamCluster.getEdgeNum(cluster2, cluster1);
                    outerCut = streamCluster.getEdgeNum(cluster1, cluster2);
                    if (innerCut != 0 || outerCut != 0) {
                        if (!clusterNeighbours.containsKey(cluster1))
                            clusterNeighbours.put(cluster1, new HashSet<>());
                        if (!clusterNeighbours.containsKey(cluster2))
                            clusterNeighbours.put(cluster2, new HashSet<>());

                        clusterNeighbours.get(cluster1).add(cluster2);
                        clusterNeighbours.get(cluster2).add(cluster1);
                    }
                    cutPart += outerCut;
                }

                if (!cutCostValue.containsKey(cluster1)) cutCostValue.put(cluster1, 0);
                cutCostValue.put(cluster1, cutCostValue.get(cluster1) + innerCut + outerCut);
            }
        }

        this.beta = GlobalConfig.getPartitionNum() * GlobalConfig.getPartitionNum() * cutPart / (sizePart * sizePart);
    }

    @Override
    public double computeCost(int clusterId, int partition) {

        double loadPart = 0.0;
        int edgeCutPart = cutCostValue.get(clusterId);
        int old_partition = clusterPartition.get(clusterId);

        loadPart = partitionLoad[old_partition];

        if (partition != old_partition)
            loadPart = partitionLoad[partition] + streamCluster.getEdgeNum(clusterId, clusterId);

        if (clusterNeighbours.containsKey(clusterId)) {
            for (Integer neighbour : clusterNeighbours.get(clusterId)) {
                if (clusterPartition.get(neighbour) == partition)
                    edgeCutPart = edgeCutPart - streamCluster.getEdgeNum(clusterId, neighbour)
                            - streamCluster.getEdgeNum(neighbour, clusterId);
            }
        }

        double alpha = GlobalConfig.getAlpha(), k = GlobalConfig.getPartitionNum();
        double m = streamCluster.getEdgeNum(clusterId, clusterId);

        return alpha * beta / k * loadPart * m + (1 - alpha) / 2 * edgeCutPart;
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
                    partitionLoad[minPartition] += streamCluster.getEdgeNum(clusterId, clusterId);
                    partitionLoad[clusterPartition.get(clusterId)] -= streamCluster.getEdgeNum(clusterId, clusterId);
                    clusterPartition.put(clusterId, minPartition);
                }
            }
            roundCnt++;
        }
    }

    public int getRoundCnt() {
        return roundCnt;
    }

    public HashMap<Integer, Integer> getClusterPartition() {
        return clusterPartition;
    }
}
