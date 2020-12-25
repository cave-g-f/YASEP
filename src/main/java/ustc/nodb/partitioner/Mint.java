package ustc.nodb.partitioner;

import ustc.nodb.Graph.Graph;
import ustc.nodb.core.Edge;
import ustc.nodb.properties.GlobalConfig;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;

public class Mint implements PartitionStrategy {

    private final HashMap<Edge, Integer> edgePartition;
    private final HashMap<Integer, HashSet<Integer>> replicateTable;
    private final int[] partitionLoad;


    public Mint(HashMap<Edge, Integer> edgePartition) {
        this.edgePartition = edgePartition;
        replicateTable = new HashMap<>();
        partitionLoad = new int[GlobalConfig.getPartitionNum()];
    }

    @Override
    public void performStep() {
        edgePartition.forEach((k, v) -> {
            partitionLoad[v]++;
            if (!replicateTable.containsKey(k.getSrcVId())) replicateTable.put(k.getSrcVId(), new HashSet<>());
            if (!replicateTable.containsKey(k.getDestVId())) replicateTable.put(k.getDestVId(), new HashSet<>());

            replicateTable.get(k.getSrcVId()).add(v);
            replicateTable.get(k.getDestVId()).add(v);
        });
    }

    @Override
    public void clear() {
        replicateTable.clear();
    }

    @Override
    public double getReplicateFactor() {
        double sum = 0.0;
        for (Integer integer : replicateTable.keySet()) {
            sum += replicateTable.get(integer).size();
        }
        return sum / GlobalConfig.getVCount();
    }

    @Override
    public double getLoadBalance() {
        double maxLoad = 0.0;
        for (int i = 0; i < GlobalConfig.getPartitionNum(); i++) {
            if(maxLoad < partitionLoad[i]){
                maxLoad = partitionLoad[i];
            }
        }
        return (double)GlobalConfig.getPartitionNum() / GlobalConfig.getECount() * maxLoad;
    }

    @Override
    public void output() throws IOException {

    }
}
