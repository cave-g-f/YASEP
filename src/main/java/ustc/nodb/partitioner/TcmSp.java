package ustc.nodb.partitioner;

import ustc.nodb.cluster.StreamCluster;
import ustc.nodb.core.Edge;
import ustc.nodb.core.Graph;
import ustc.nodb.game.ClusterPackGame;
import ustc.nodb.properties.GlobalConfig;
import ustc.nodb.sketch.GraphSketch;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;

public class TcmSp implements PartitionStrategy {

    private final Graph graph;
    private final GraphSketch graphSketch;
    private final StreamCluster streamCluster;
    private final ClusterPackGame clusterPackGame;
    private final ArrayList<HashSet<Edge>> partitionTable;
    private final HashMap<Integer, HashSet<Integer>> replicateTable;

    public TcmSp(Graph graph, GraphSketch graphSketch, StreamCluster streamCluster, ClusterPackGame clusterPackGame) {
        this.graph = graph;
        this.graphSketch = graphSketch;
        this.streamCluster = streamCluster;
        this.clusterPackGame = clusterPackGame;
        partitionTable = new ArrayList<>();
        for (int i = 0; i < GlobalConfig.getPartitionNum(); i++)
            partitionTable.add(new HashSet<>());
        replicateTable = new HashMap<>();
    }

    @Override
    public void performStep() {

        for (Edge edge : graph.getEdgeList()) {
            int src = edge.getSrcVId();
            int dest = edge.getDestVId();
            int srcHash = graphSketch.hashVertex(src);
            int destHash = graphSketch.hashVertex(dest);
            int srcPartition = clusterPackGame.getClusterPartition(streamCluster.getClusterId(srcHash));
            int destPartition = clusterPackGame.getClusterPartition(streamCluster.getClusterId(destHash));
            int edgePartition = -1;

            if (!replicateTable.containsKey(src)) replicateTable.put(src, new HashSet<>());
            if (!replicateTable.containsKey(dest)) replicateTable.put(dest, new HashSet<>());
            if (srcPartition == destPartition)
                edgePartition = srcPartition;
            else {
                if (graphSketch.getDegree(srcHash) > graphSketch.getDegree(destHash)) {
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
}
