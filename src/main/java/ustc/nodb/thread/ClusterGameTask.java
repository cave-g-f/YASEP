package ustc.nodb.thread;

import ustc.nodb.cluster.StreamCluster;
import ustc.nodb.game.ClusterPackGame;
import ustc.nodb.properties.GlobalConfig;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.Callable;

public class ClusterGameTask implements Callable<ClusterPackGame> {

    private final StreamCluster streamCluster;
    private final ArrayList<Integer> cluster;

    public ClusterGameTask(StreamCluster streamCluster, int taskId) {
        this.streamCluster = streamCluster;

        int batchSize = GlobalConfig.getBatchSize();
        ArrayList<Integer> clusterList = streamCluster.getClusterList();
        int begin = batchSize * taskId;
        int end = Math.min(batchSize * (taskId + 1), clusterList.size());

        this.cluster = new ArrayList<>();
        for (int i = begin; i < end; i++) {
            this.cluster.add(clusterList.get(i));
        }
    }

    @Override
    public ClusterPackGame call() throws Exception {
        ClusterPackGame clusterPackGame = new ClusterPackGame(streamCluster, cluster);
        clusterPackGame.initGame();
        clusterPackGame.startGame();
        return clusterPackGame;
    }
}
