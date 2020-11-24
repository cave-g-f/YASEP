package ustc.nodb.sketch;

import org.junit.Test;
import ustc.nodb.Graph.OriginGraph;
import ustc.nodb.Graph.SketchGraph;

public class OriginSketchGraphTest {

    OriginGraph originGraph;
    SketchGraph sketchGraph;

    public OriginSketchGraphTest() {
        originGraph = new OriginGraph();
        originGraph.readGraphFromFile();
        sketchGraph = new SketchGraph(0);
    }

    @Test
    public void testAdjMatrix(){
        sketchGraph.readGraphFromFile();
        System.out.println(sketchGraph);
    }

    @Test
    public void testGetAdjMatrix(){
        sketchGraph.readGraphFromFile();
        int[][] adj = sketchGraph.getAdjMatrix();
        System.out.println(sketchGraph.getAdjMatrix().hashCode());
    }
}