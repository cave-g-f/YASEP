package ustc.nodb.partitioner;

import ustc.nodb.Graph.Graph;
import ustc.nodb.cluster.StreamCluster;
import ustc.nodb.core.Edge;
import ustc.nodb.Graph.OriginGraph;
import ustc.nodb.game.ClusterPackGame;
import ustc.nodb.properties.GlobalConfig;
import ustc.nodb.Graph.SketchGraph;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

public class TcmSp implements PartitionStrategy {

    private final Graph originGraph;
    private final StreamCluster streamCluster;
    private final ClusterPackGame clusterPackGame;
    private final ArrayList<HashSet<Edge>> partitionTable;
    private final HashMap<Integer, HashSet<Integer>> replicateTable;

    public TcmSp(Graph originGraph, StreamCluster streamCluster, ClusterPackGame clusterPackGame) {
        this.originGraph = originGraph;
        this.streamCluster = streamCluster;
        this.clusterPackGame = clusterPackGame;
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
            int srcPartition = clusterPackGame.getClusterPartition(streamCluster.getClusterId(src));
            int destPartition = clusterPackGame.getClusterPartition(streamCluster.getClusterId(dest));
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

    public double getReplicateFactor(){
        double sum = 0.0;
        for (Integer integer : replicateTable.keySet()) {
            sum += replicateTable.get(integer).size();
        }
        return sum / GlobalConfig.getVCount();
    }

    public double getLoadBalance(){
        double averageLoad = 0.0;
        double sigma = 0.0;
        for(HashSet<Edge> edgeSet : partitionTable){
            averageLoad += edgeSet.size();
        }
        averageLoad /= GlobalConfig.getPartitionNum();
        for(HashSet<Edge> edgeSet : partitionTable){
            sigma += Math.pow(edgeSet.size() - averageLoad, 2);
        }
        sigma /= GlobalConfig.getPartitionNum();
        return Math.sqrt(sigma)/averageLoad;
    }

}
