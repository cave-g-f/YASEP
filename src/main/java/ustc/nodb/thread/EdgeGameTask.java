package ustc.nodb.thread;

import org.checkerframework.checker.units.qual.A;
import ustc.nodb.Graph.Graph;
import ustc.nodb.core.Edge;
import ustc.nodb.game.EdgePartitionGame;
import ustc.nodb.properties.GlobalConfig;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.Callable;

public class EdgeGameTask implements Callable<HashMap<Edge, Integer>> {

    private final ArrayList<Edge> edges;

    public EdgeGameTask(Graph graph, int taskId) {
        ArrayList<Edge> edgeList = graph.getEdgeList();
        edges = new ArrayList<>();

        int batchSize = GlobalConfig.getBatchSize();
        int begin = batchSize * taskId;
        int end = Math.min(batchSize * (taskId + 1), edgeList.size());

        for (int i = begin; i < end; i++) {
            this.edges.add(edgeList.get(i));
        }
    }

    @Override
    public HashMap<Edge, Integer> call() throws Exception {
        EdgePartitionGame game = new EdgePartitionGame(edges);
        game.initGame();
        game.startGame();
        return game.getEdgePartition();
    }
}
