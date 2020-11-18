package ustc.nodb.game;

import ustc.nodb.cluster.StreamCluster;
import ustc.nodb.properties.GlobalConfig;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Random;

public class ClusterPackGame {

    private final HashMap<Integer, Integer> clusterPartition; // key: cluster value: partition
    private final ArrayList<HashSet<Integer>> invertedPartitionIndex; // key: partition value: cluster list
    private final ArrayList<Integer> clusterList;
    private final StreamCluster streamCluster;
    private double beta;

    public ClusterPackGame(StreamCluster streamCluster) {
        this.clusterPartition = new HashMap<>();
        this.streamCluster = streamCluster;
        this.clusterList = streamCluster.getClusterList();
        this.invertedPartitionIndex = new ArrayList<>();
        for (int i = 0; i < GlobalConfig.getPartitionNum(); i++) {
            this.invertedPartitionIndex.add(new HashSet<>());
        }
    }

    private void initGame() {
        Random random = new Random();
        for (Integer clusterId : clusterList) {
            int partition = random.nextInt(GlobalConfig.getPartitionNum());
            clusterPartition.put(clusterId, partition);
            invertedPartitionIndex.get(partition).add(clusterId);
        }

        double cutPart = 0.0, sizePart = 0.0;
        for(Integer cluster1 : clusterList){
            for(Integer cluster2 : clusterList){
                if(cluster1.equals(cluster2)) sizePart += streamCluster.getEdgeWeight(cluster1, cluster1);
                else cutPart += streamCluster.getEdgeWeight(cluster1, cluster2);
            }
        }

        this.beta = GlobalConfig.getPartitionNum() * GlobalConfig.getPartitionNum() * cutPart / (sizePart * sizePart);
    }

    private double computeCost(int clusterId, int partition) {

        double loadPart = 0.0, edgeCutPart = 0.0;

        for (Integer otherCluster : clusterList) {
            if (clusterPartition.get(otherCluster) == partition || otherCluster == clusterId){
                loadPart += streamCluster.getEdgeWeight(otherCluster, otherCluster);
                continue;
            }
            edgeCutPart += streamCluster.getEdgeWeight(clusterId, otherCluster)
                    + streamCluster.getEdgeWeight(otherCluster, clusterId);
        }

        double alpha = GlobalConfig.getAlpha(), k = GlobalConfig.getPartitionNum();
        double m = streamCluster.getEdgeWeight(clusterId, clusterId);

        return alpha * beta / k * loadPart * m + (1 - alpha) / 2 * edgeCutPart;
    }

    public void startGame() {

        initGame();
        boolean finish = false;

        while (!finish) {
            finish = false;
            for (Integer clusterId : clusterList) {
                double minCost = Double.MAX_VALUE;
                int minPartition = clusterPartition.get(clusterId);

                for (int j = 0; j < GlobalConfig.getPartitionNum(); j++) {
                    double cost = computeCost(clusterId, j);
                    if(cost < minCost){
                        minCost = cost;
                        minPartition = j;
                    }
                }

                if(minPartition != clusterPartition.get(clusterId)){
                    finish = true;
                    invertedPartitionIndex.get(clusterPartition.get(clusterId)).remove(clusterId);
                    clusterPartition.put(clusterId, minPartition);
                    invertedPartitionIndex.get(minPartition).add(clusterId);
                }
            }
        }
    }

    public ArrayList<HashSet<Integer>> getInvertedPartitionIndex() {
        return invertedPartitionIndex;
    }
}
