package ustc.nodb.thread;

import org.junit.Test;
import ustc.nodb.cluster.StreamCluster;
import ustc.nodb.Graph.OriginGraph;
import ustc.nodb.properties.GlobalConfig;
import ustc.nodb.Graph.SketchGraph;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.*;

public class ClusterTaskTest {

    OriginGraph originGraph;
    ArrayList<SketchGraph> sketchGraphs = new ArrayList<>();
    ArrayList<StreamCluster> streamClusters = new ArrayList<>();

    public ClusterTaskTest() {
        originGraph = new OriginGraph();
        originGraph.readGraphFromFile();
    }

    public void testSketchTask() throws InterruptedException, ExecutionException {
        ExecutorService taskPool = Executors.newCachedThreadPool();
        CompletionService<SketchGraph> completionService = new ExecutorCompletionService<>(taskPool);

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

//        for(GraphSketch sketch : graphSketches){
//            System.out.println(sketch);
//        }
    }

    @Test
    public void testClusterTask() throws ExecutionException, InterruptedException {

        testSketchTask();

        ExecutorService taskPool = Executors.newCachedThreadPool();
        CompletionService<StreamCluster> completionService = new ExecutorCompletionService<>(taskPool);

        for(int i = 0; i < GlobalConfig.getHashNum(); i++){
            completionService.submit(new ClusterTask(sketchGraphs.get(i), i));
        }

        for (int i = 0; i < GlobalConfig.getHashNum(); i++) {
            try {
                Future<StreamCluster> result = completionService.take();
                streamClusters.add(result.get());

            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }
        }

        for(StreamCluster cluster : streamClusters){
            System.out.println(cluster);
        }

        for(StreamCluster cluster : streamClusters){
            HashMap<Integer, HashMap<Integer, Integer>> map = cluster.getInnerAndCutEdge();

            map.forEach((k1, v1)->{
                v1.forEach((k2, v2)->{
                    System.out.println(k1.toString() + " : " + k2.toString() + " : " + v2.toString());
                });
            });
        }
    }

}