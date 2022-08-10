package ustc.nodb.cluster;

import ustc.nodb.Graph.Graph;
import ustc.nodb.core.Edge;
import ustc.nodb.properties.GlobalConfig;

import java.util.*;

public class Holl {

    private final int[] cluster;
    private final int[] degree;
    private final HashMap<Integer, Long> volume;
    private final Graph graph;
    private final List<Integer> clusterList;
    private final int maxVolume;

    public Holl(Graph graph) {
        this.cluster = new int[GlobalConfig.vCount];
        this.graph = graph;
        this.volume = new HashMap<>();
        this.maxVolume = 2 * GlobalConfig.getMaxClusterVolume();
        this.clusterList = new ArrayList<>();
        this.degree = new int[GlobalConfig.vCount];
    }

    public void preStreamCluster() {

        graph.readGraphFromFile();
        Edge edge;
        while ((edge = graph.readStep()) != null) {
            int src = edge.getSrcVId();
            int dest = edge.getDestVId();
            degree[src] += 1;
            degree[dest] += 1;
        }
        graph.clear();

    }

    public void startSteamCluster() {

        int clusterID = 1;

        graph.readGraphFromFile();

        Edge edge;

        while ((edge = graph.readStep()) != null) {

            int src = edge.getSrcVId();
            int dest = edge.getDestVId();

            // allocate cluster
            if (cluster[src] == 0) cluster[src] = clusterID++;
            if (cluster[dest] == 0) cluster[dest] = clusterID++;

            // update volume
            if (!volume.containsKey(cluster[src])) {
                volume.put(cluster[src], 0L);
            }
            if (!volume.containsKey(cluster[dest])) {
                volume.put(cluster[dest], 0L);
            }

            volume.put(cluster[src], volume.get(cluster[src]) + degree[src]);
            volume.put(cluster[dest], volume.get(cluster[dest]) + degree[dest]);

            long volU = volume.get(cluster[src]);
            long volV = volume.get(cluster[dest]);
            long realVolU = volU - degree[src];
            long realVolV = volV - degree[dest];

            if (volU <= maxVolume && volV <= maxVolume) {
                if (realVolU <= realVolV && volV + degree[src] <= maxVolume) {
                    volume.put(cluster[src], volume.get(cluster[src]) - degree[src]);
                    volume.put(cluster[dest], volume.get(cluster[dest]) + degree[src]);
                    cluster[src] = cluster[dest];
                } else if (realVolV < realVolU && volU + degree[dest] <= maxVolume) {
                    volume.put(cluster[dest], volume.get(cluster[dest]) - degree[dest]);
                    volume.put(cluster[src], volume.get(cluster[src]) + degree[dest]);
                    cluster[dest] = cluster[src];
                }
            }
        }

        List<HashMap.Entry<Integer, Long>> sortList = new ArrayList<HashMap.Entry<Integer, Long>>(volume.entrySet());
        Collections.sort(sortList, new Comparator<HashMap.Entry<Integer, Long>>() {
            @Override
            public int compare(Map.Entry<Integer, Long> t1, Map.Entry<Integer, Long> t2) {
                return Long.compare(t1.getValue(), t2.getValue()) * -1;
            }
        });

        for (int i = 0; i < sortList.size(); i++) {
            int c = sortList.get(i).getKey();
            if(volume.get(c) == 0) break;
            this.clusterList.add(c);
        }

        graph.clear();
    }

    @Override
    public String toString() {
        StringBuilder volumeStr = new StringBuilder();
        StringBuilder clusterStr = new StringBuilder();
        volume.forEach((k, v) -> {
            volumeStr.append("cluster ").append(k).append(" volume: ").append(v).append("\n");
        });

        return volumeStr.toString() + clusterStr.toString();
    }

    public List<Integer> getClusterList() {
        return clusterList;
    }

    public int getClusterId(int vId) {
        return cluster[vId];
    }

    public Long getVolume(int cluster) {
        return volume.get(cluster);
    }

    public int getDegree(int vid) {
        return degree[vid];
    }
}
