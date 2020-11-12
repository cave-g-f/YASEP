package ustc.nodb.core;

import org.junit.Test;

public class GraphTest {

    Graph graph = new Graph();

    @Test
    public void testEdgeNum() {
        graph.readGraphFromFile();
        System.out.println(graph.getEdgeList().size());
        assert (graph.getEdgeList().size() == graph.getECount());
    }

}