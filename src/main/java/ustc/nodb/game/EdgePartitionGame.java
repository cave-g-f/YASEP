package ustc.nodb.game;

import ustc.nodb.core.Edge;
import ustc.nodb.properties.GlobalConfig;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Random;

public class EdgePartitionGame implements GameStrategy {

    private final ArrayList<Edge> edgeList;
    private final HashMap<Integer, Integer> reflectTable;
    private final HashMap<Edge, Integer> edgePartition;
    private final int[] partitionLoad;
    private int[][] vertexRecordTable;
    private double beta;

    public EdgePartitionGame(ArrayList<Edge> edgeList) {
        this.edgeList = edgeList;
        reflectTable = new HashMap<>();
        partitionLoad = new int[GlobalConfig.getPartitionNum()];
        edgePartition = new HashMap<>();
    }

    @Override
    public void initGame() {
        int vertexCount = 0;

        // init statistics
        for (Edge edge : edgeList) {
            int src = edge.getSrcVId();
            int dest = edge.getDestVId();

            if (!reflectTable.containsKey(src)) reflectTable.put(src, vertexCount++);
            if (!reflectTable.containsKey(dest)) reflectTable.put(dest, vertexCount++);
        }

        vertexRecordTable = new int[vertexCount][GlobalConfig.getPartitionNum()];

        // init random partition
        Random random = new Random();

        for (Edge edge : edgeList) {
            int partition = random.nextInt(GlobalConfig.getPartitionNum());
            int src = reflectTable.get(edge.getSrcVId());
            int dest = reflectTable.get(edge.getDestVId());

            if (!edgePartition.containsKey(edge)) edgePartition.put(edge, partition);

            partitionLoad[partition]++;
            vertexRecordTable[src][partition]++;
            vertexRecordTable[dest][partition]++;
        }

        beta = Math.pow(GlobalConfig.getPartitionNum(), 3) * (double) vertexCount / (2 * Math.pow(edgeList.size(), 2));
    }

    private double computeCost(Edge edge, int partition) {
        double alpha = GlobalConfig.getAlpha();
        int k = GlobalConfig.getPartitionNum();

        int src = reflectTable.get(edge.getSrcVId());
        int dest = reflectTable.get(edge.getDestVId());

        int load = partitionLoad[partition];
        double srcDegreeCost = vertexRecordTable[src][partition];
        double destDegreeCost = vertexRecordTable[dest][partition];

        if(partition != edgePartition.get(edge)){
            load++;
            srcDegreeCost++;
            destDegreeCost++;
        }

        return alpha * beta / k * load + (1 - alpha) * (1 / srcDegreeCost + 1 / destDegreeCost);
    }

    @Override
    public void startGame() {

        boolean finish = false;

        while (!finish) {
            finish = true;
            for (Edge edge : edgeList) {
                double minCost = Integer.MAX_VALUE;
                int originPartition = edgePartition.get(edge);
                int src = reflectTable.get(edge.getSrcVId());
                int dest = reflectTable.get(edge.getDestVId());
                int minPartition = 0;

                for (int i = 0; i < GlobalConfig.getPartitionNum(); i++) {
                    double cost = computeCost(edge, i);
                    if (minCost > cost) {
                        minCost = cost;
                        minPartition = i;
                    } else if (minCost == cost && i < minPartition) minPartition = i;
                }

                if (minPartition != originPartition) finish = false;

                partitionLoad[originPartition]--;
                vertexRecordTable[src][originPartition]--;
                vertexRecordTable[dest][originPartition]--;
                partitionLoad[minPartition]++;
                vertexRecordTable[src][minPartition]++;
                vertexRecordTable[dest][minPartition]++;
                edgePartition.put(edge, minPartition);
            }
        }
    }

    public HashMap<Edge, Integer> getEdgePartition(){
        return this.edgePartition;
    }
}
