package ustc.nodb.thread;

import org.junit.Test;
import ustc.nodb.Graph.OriginGraph;
import ustc.nodb.properties.GlobalConfig;
import ustc.nodb.Graph.SketchGraph;

import java.util.ArrayList;
import java.util.concurrent.*;

public class SketchTaskTest {

    OriginGraph originGraph;

    public SketchTaskTest() {
        originGraph = new OriginGraph();
        originGraph.readGraphFromFile();
    }

    @Test
    public void testSketchTask() throws InterruptedException, ExecutionException {
        ExecutorService taskPool = Executors.newCachedThreadPool();
        CompletionService<SketchGraph> completionService = new ExecutorCompletionService<>(taskPool);
        ArrayList<SketchGraph> sketchGraphs = new ArrayList<>();

        for (int i = 0; i < GlobalConfig.getHashNum(); i++) {
            completionService.submit(new SketchTask(i));
        }

        for (int i = 0; i < GlobalConfig.getHashNum(); i++) {
            try {
                Future<SketchGraph> result = completionService.take();
                sketchGraphs.add(result.get());

            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        for(SketchGraph sketch : sketchGraphs){
            System.out.println(sketch);
        }

    }

}