package ustc.nodb.thread;

import ustc.nodb.cluster.StreamCluster;
import ustc.nodb.game.ClusterPackGame;

import java.util.concurrent.Callable;

public class GameTask implements Callable<ClusterPackGame> {

    private final StreamCluster streamCluster;
    private final int taskId;

    public GameTask(StreamCluster streamCluster, int taskId) {
        this.streamCluster = streamCluster;
        this.taskId = taskId;
    }

    @Override
    public ClusterPackGame call() throws Exception {
        ClusterPackGame clusterPackGame = new ClusterPackGame(streamCluster);
        clusterPackGame.startGame();
        return clusterPackGame;
    }
}
