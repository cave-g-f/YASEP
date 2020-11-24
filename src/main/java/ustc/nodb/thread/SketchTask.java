package ustc.nodb.thread;

import ustc.nodb.Graph.OriginGraph;
import ustc.nodb.Graph.SketchGraph;

import java.util.concurrent.Callable;

public class SketchTask implements Callable<SketchGraph> {

    private final int taskId;

    public SketchTask(int taskId) {
        this.taskId = taskId;
    }

    @Override
    public SketchGraph call() throws Exception {
        SketchGraph sketchGraph = new SketchGraph(taskId);
        sketchGraph.readGraphFromFile();
        return sketchGraph;
    }
}
