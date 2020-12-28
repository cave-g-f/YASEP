package ustc.nodb.game;

import org.checkerframework.checker.units.qual.A;
import org.javatuples.Pair;
import ustc.nodb.cluster.StreamCluster;
import ustc.nodb.properties.GlobalConfig;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Random;

public class ClusterPackGame implements GameStrategy{

    private final HashMap<Integer, Integer> clusterPartition; // key: cluster value: partition
    private final ArrayList<HashSet<Integer>> invertedPartitionIndex; // key: partition value: cluster list
    private final HashMap<Integer, Double> cutCostValue; // key: cluster value: cutCost
    private final double[] partitionLoad;
    private final ArrayList<Integer> clusterList;
    private final StreamCluster streamCluster;
    private int cutEdge = 0;
    private double beta = 0.0;
    private int roundCnt;

    public ClusterPackGame(StreamCluster streamCluster, ArrayList<Integer> clusterList) {
        this.clusterPartition = new HashMap<>();
        this.streamCluster = streamCluster;
        this.clusterList = clusterList;
        this.invertedPartitionIndex = new ArrayList<>();
        for (int i = 0; i < GlobalConfig.getPartitionNum(); i++) {
            this.invertedPartitionIndex.add(new HashSet<>());
        }
        cutCostValue = new HashMap<>();
        partitionLoad = new double[GlobalConfig.getPartitionNum()];
    }

    @Override
    public void initGame() {
        Random random = new Random();
        for (Integer clusterId : clusterList) {
            int partition = random.nextInt(GlobalConfig.getPartitionNum());
            clusterPartition.put(clusterId, partition);
            invertedPartitionIndex.get(partition).add(clusterId);
        }

        double cutPart = 0.0, sizePart = 0.0;
        for (Integer cluster1 : clusterList) {
            double cutCost=0.0;
            partitionLoad[clusterPartition.get(cluster1)] += streamCluster.getEdgeNum(cluster1, cluster1);
            sizePart += streamCluster.getEdgeNum(cluster1, cluster1);
            for (Integer cluster2 : clusterList) {
                if (!cluster1.equals(cluster2)) cutPart += streamCluster.getEdgeNum(cluster1, cluster2);
                if (!clusterPartition.get(cluster1).equals(clusterPartition.get(cluster2)))
                    cutCost += streamCluster.getEdgeNum(cluster1, cluster2) + streamCluster.getEdgeNum(cluster2, cluster1);
            }
            cutCostValue.put(cluster1, cutCost);
        }

        this.beta = GlobalConfig.getPartitionNum() * GlobalConfig.getPartitionNum() * cutPart / (sizePart * sizePart);
    }

    private double computeCost(int clusterId, int partition) {

        double loadPart = 0.0, edgeCutPart = 0.0;
        int old_partition = clusterPartition.get(clusterId);

        loadPart = partitionLoad[old_partition];
        edgeCutPart = cutCostValue.get(clusterId);

        if(partition != old_partition){
            loadPart = partitionLoad[partition] + streamCluster.getEdgeNum(clusterId, clusterId);

            // update cut edge cost value
            for(Integer otherCluster : invertedPartitionIndex.get(old_partition)){
                if(otherCluster == clusterId) continue;
                edgeCutPart += streamCluster.getEdgeNum(clusterId, otherCluster)
                        + streamCluster.getEdgeNum(otherCluster, clusterId);
            }
            for(Integer otherCluster : invertedPartitionIndex.get(partition)){
                edgeCutPart -= streamCluster.getEdgeNum(clusterId, otherCluster)
                        - streamCluster.getEdgeNum(otherCluster, clusterId);
            }
        }

        double alpha = GlobalConfig.getAlpha(), k = GlobalConfig.getPartitionNum();
        double m = streamCluster.getEdgeNum(clusterId, clusterId);

        return alpha * beta / k * loadPart * m + (1 - alpha) / 2 * edgeCutPart;
    }

    private void computeCutEdge() {
        this.cutEdge = 0;
        for (Integer cluster1 : clusterList) {
            for (Integer cluster2 : clusterList) {
                if (cluster1.equals(cluster2) || clusterPartition.get(cluster1).equals(clusterPartition.get(cluster2)))
                    continue;
                this.cutEdge += streamCluster.getEdgeNum(cluster1, cluster2);
            }
        }
    }

    @Override
    public void startGame() {
        boolean finish = false;

        long startTime = System.currentTimeMillis();
        while (!finish) {
            finish = true;
            roundCnt++;
            for (Integer clusterId : clusterList) {
                double minCost = Double.MAX_VALUE;
                int minPartition = clusterPartition.get(clusterId);

                for (int j = 0; j < GlobalConfig.getPartitionNum(); j++) {
                    double cost = computeCost(clusterId, j);
                    if (cost < minCost) {
                        minCost = cost;
                        minPartition = j;
                    }
                }

                if (minPartition != clusterPartition.get(clusterId)) {
                    finish = false;

                    // update partition load
                    partitionLoad[minPartition] += streamCluster.getEdgeNum(clusterId, clusterId);
                    partitionLoad[clusterPartition.get(clusterId)] -= streamCluster.getEdgeNum(clusterId, clusterId);

                    // update cut cost
                    invertedPartitionIndex.get(clusterPartition.get(clusterId)).remove(clusterId);
                    for(Integer otherCluster : invertedPartitionIndex.get(clusterPartition.get(clusterId))){
                        double cutCost1 = streamCluster.getEdgeNum(clusterId, otherCluster);
                        double cutCost2 = streamCluster.getEdgeNum(otherCluster, clusterId);
                        cutCostValue.put(clusterId, cutCostValue.get(clusterId) + cutCost1 + cutCost2);
                        cutCostValue.put(otherCluster, cutCostValue.get(otherCluster) + cutCost2 + cutCost1);
                    }
                    for(Integer otherCluster : invertedPartitionIndex.get(minPartition)){
                        double cutCost1 = streamCluster.getEdgeNum(clusterId, otherCluster);
                        double cutCost2 = streamCluster.getEdgeNum(otherCluster, clusterId);
                        cutCostValue.put(clusterId, cutCostValue.get(clusterId) - cutCost1 - cutCost2);
                        cutCostValue.put(otherCluster, cutCostValue.get(otherCluster) - cutCost2 - cutCost1);
                    }

                    clusterPartition.put(clusterId, minPartition);
                    invertedPartitionIndex.get(minPartition).add(clusterId);
                }
            }
        }
//        long endTime = System.currentTimeMillis();
//        System.out.println("round time : " + (endTime - startTime) + " ms");

        computeCutEdge();
    }

    public ArrayList<HashSet<Integer>> getInvertedPartitionIndex() {
        return invertedPartitionIndex;
    }

    public int getCutEdge() {
        return cutEdge;
    }

    public HashMap<Integer, Integer> getClusterPartition() {
        return clusterPartition;
    }

    public int getRoundCnt() {
        return roundCnt;
    }
}
