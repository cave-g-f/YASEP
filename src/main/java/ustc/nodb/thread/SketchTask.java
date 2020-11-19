package ustc.nodb.thread;

import ustc.nodb.core.Graph;
import ustc.nodb.sketch.GraphSketch;

import java.util.concurrent.Callable;

public class SketchTask implements Callable<GraphSketch> {

    private final Graph graph;
    private final int taskId;

    public SketchTask(Graph graph, int taskId) {
        this.graph = graph;
        this.taskId = taskId;
    }

    @Override
    public GraphSketch call() throws Exception {
        GraphSketch graphSketch = new GraphSketch(graph, taskId);
        graphSketch.setupAdjMatrix();
        return graphSketch;
    }
}
