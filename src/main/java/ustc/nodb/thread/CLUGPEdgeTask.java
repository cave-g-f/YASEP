package ustc.nodb.thread;

import java.util.concurrent.*;
import ustc.nodb.partitioner.*;
import ustc.nodb.cluster.*;
import ustc.nodb.IO.*;
import ustc.nodb.properties.*;
import ustc.nodb.game.*;
import java.util.*;

public class CLUGPEdgeTask implements Callable<CLUGPEdge>
{
    private final EdgeStreamCluster edgeStreamCluster;
    private final HashSet<Integer> cluster;
    private final BatchFileIO batchFileIO;
    private int taskId;
    
    public CLUGPEdgeTask(final EdgeStreamCluster edgeStreamCluster, final BatchFileIO batchFileIO, final int taskId) {
        this.edgeStreamCluster = edgeStreamCluster;
        this.taskId = taskId;
        this.batchFileIO = batchFileIO;
        this.cluster = new HashSet<Integer>();
        final int batchSize = GlobalConfig.getBatchSize();
        final int begin = batchSize * taskId;
        final int end = Math.min(batchSize * (taskId + 1), edgeStreamCluster.getClusterList().size());
        this.cluster.addAll((Collection<?>)edgeStreamCluster.getClusterList().subList(begin, end));
    }
    
    @Override
    public CLUGPEdge call() throws Exception {
        final ClusterPackEdgeGame clusterPackEdgeGame = new ClusterPackEdgeGame(this.edgeStreamCluster, this.cluster, this.taskId);
        clusterPackEdgeGame.initGame();
        clusterPackEdgeGame.startGame();
        final HashMap<Integer, Integer> clusterPartition = clusterPackEdgeGame.getClusterPartition();
        final CLUGPEdge clugpEdge = new CLUGPEdge(this.edgeStreamCluster, clusterPartition, this.batchFileIO, this.taskId);
        clugpEdge.performStep();
        return clugpEdge;
    }
}
