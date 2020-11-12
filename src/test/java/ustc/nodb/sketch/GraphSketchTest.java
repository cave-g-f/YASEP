package ustc.nodb.sketch;

import org.junit.Test;
import ustc.nodb.core.Graph;

public class GraphSketchTest {

    Graph graph;
    GraphSketch graphSketch;

    public GraphSketchTest() {
        graph = new Graph();
        graph.readGraphFromFile();
        graphSketch = new GraphSketch(graph);
    }

    @Test
    public void testAdjMatrix(){
        graphSketch.setupAdjMatrix(1);
        System.out.println(graphSketch);
    }
}