package ustc.nodb.thread;

import ustc.nodb.cluster.StreamCluster;
import ustc.nodb.sketch.GraphSketch;

import java.util.concurrent.Callable;

public class ClusterTask implements Callable<StreamCluster> {

    private final GraphSketch sketch;
    private final int taskId;

    public ClusterTask(GraphSketch sketch, int taskId) {
        this.sketch = sketch;
        this.taskId = taskId;
    }

    @Override
    public StreamCluster call() throws Exception {
        StreamCluster streamCluster = new StreamCluster(this.sketch);
        streamCluster.startSteamCluster();
        return streamCluster;
    }
}
