package ustc.nodb.thread;

import org.junit.Test;
import ustc.nodb.core.Graph;
import ustc.nodb.properties.GlobalConfig;
import ustc.nodb.sketch.GraphSketch;

import java.util.ArrayList;
import java.util.concurrent.*;

public class SketchTaskTest {

    Graph graph;

    public SketchTaskTest() {
        graph = new Graph();
        graph.readGraphFromFile();
    }

    @Test
    public void testSketchTask() throws InterruptedException, ExecutionException {
        ExecutorService taskPool = Executors.newCachedThreadPool();
        CompletionService<GraphSketch> completionService = new ExecutorCompletionService<>(taskPool);
        ArrayList<GraphSketch> graphSketches = new ArrayList<>();

        for (int i = 0; i < GlobalConfig.getHashNum(); i++) {
            completionService.submit(new SketchTask(graph, i));
        }

        for (int i = 0; i < GlobalConfig.getHashNum(); i++) {
            try {
                Future<GraphSketch> result = completionService.take();
                graphSketches.add(result.get());

            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        for(GraphSketch sketch : graphSketches){
            System.out.println(sketch);
        }

    }

}