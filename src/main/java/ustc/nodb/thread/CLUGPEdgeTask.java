package ustc.nodb.thread;

import ustc.nodb.IO.BatchFileIO;
import ustc.nodb.cluster.EdgeStreamCluster;
import ustc.nodb.game.ClusterPackEdgeGame;
import ustc.nodb.partitioner.CLUGPEdge;
import ustc.nodb.properties.GlobalConfig;

import java.util.HashMap;
import java.util.HashSet;
import java.util.concurrent.Callable;

public class CLUGPEdgeTask implements Callable<CLUGPEdge> {

    private final EdgeStreamCluster edgeStreamCluster;
    private final HashSet<Integer> cluster;
    private final BatchFileIO batchFileIO;
    private int taskId;

    public CLUGPEdgeTask(EdgeStreamCluster edgeStreamCluster, BatchFileIO batchFileIO, int taskId) {
        this.edgeStreamCluster = edgeStreamCluster;
        this.taskId = taskId;
        this.batchFileIO = batchFileIO;

        this.cluster = new HashSet<>();
        int batchSize = GlobalConfig.getBatchSize();
        int begin = batchSize * taskId;
        int end = Math.min(batchSize * (taskId + 1), edgeStreamCluster.getClusterList().size());
        this.cluster.addAll(edgeStreamCluster.getClusterList().subList(begin, end));
    }

    @Override
    public CLUGPEdge call() throws Exception {
        ClusterPackEdgeGame clusterPackEdgeGame = new ClusterPackEdgeGame(edgeStreamCluster, cluster, taskId);
        clusterPackEdgeGame.initGame();
        clusterPackEdgeGame.startGame();
        HashMap<Integer, Integer> clusterPartition = clusterPackEdgeGame.getClusterPartition();
        CLUGPEdge clugpEdge = new CLUGPEdge(edgeStreamCluster, clusterPartition, batchFileIO, taskId);
        clugpEdge.performStep();
        return clugpEdge;
    }
}
