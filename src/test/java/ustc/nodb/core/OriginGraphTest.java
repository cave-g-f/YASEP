package ustc.nodb.core;

import org.junit.Test;
import ustc.nodb.Graph.OriginGraph;

public class OriginGraphTest {

    OriginGraph originGraph = new OriginGraph();

    @Test
    public void testEdgeNum() {
        originGraph.readGraphFromFile();
        System.out.println(originGraph.getEdgeList().size());
        assert (originGraph.getEdgeList().size() == originGraph.getECount());
    }

}