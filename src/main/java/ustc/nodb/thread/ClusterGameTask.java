package ustc.nodb.thread;

import java.util.concurrent.*;
import ustc.nodb.game.*;
import ustc.nodb.cluster.*;
import java.util.*;
import ustc.nodb.properties.*;

public class ClusterGameTask implements Callable<ClusterPackGame>
{
    private final StreamCluster streamCluster;
    private final List<Integer> cluster;
    
    public ClusterGameTask(final StreamCluster streamCluster, final int taskId) {
        this.streamCluster = streamCluster;
        final int batchSize = GlobalConfig.getBatchSize();
        final List<Integer> clusterList = streamCluster.getClusterList();
        final int begin = batchSize * taskId;
        final int end = Math.min(batchSize * (taskId + 1), clusterList.size());
        this.cluster = clusterList.subList(begin, end);
    }
    
    @Override
    public ClusterPackGame call() throws Exception {
        final ClusterPackGame clusterPackGame = new ClusterPackGame(this.streamCluster, this.cluster);
        clusterPackGame.initGame();
        clusterPackGame.startGame();
        return clusterPackGame;
    }
}
