package ustc.nodb.sketch;

import org.junit.Test;
import ustc.nodb.core.Graph;

public class GraphSketchTest {

    Graph graph;
    GraphSketch graphSketch;

    public GraphSketchTest() {
        graph = new Graph();
        graph.readGraphFromFile();
        graphSketch = new GraphSketch(graph, 0);
    }

    @Test
    public void testAdjMatrix(){
        graphSketch.setupAdjMatrix();
        System.out.println(graphSketch);
    }

    @Test
    public void testGetAdjMatrix(){
        graphSketch.setupAdjMatrix();
        int[][] adj = graphSketch.getAdjMatrix();
        System.out.println(graphSketch.getAdjMatrix().hashCode());
    }
}