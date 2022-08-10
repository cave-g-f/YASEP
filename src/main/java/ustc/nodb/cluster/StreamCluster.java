package ustc.nodb.cluster;

import ustc.nodb.Graph.Graph;
import ustc.nodb.core.Edge;
import ustc.nodb.properties.GlobalConfig;

import java.util.*;

public class StreamCluster {

    private final int[] cluster;
    private final int[] degree;
    private final HashMap<Integer, Integer> volume;
    // clusterId1 = clusterId2 save inner, otherwise save cut
    private final HashMap<Integer, HashMap<Integer, Integer>> innerAndCutEdge;
    private final HashMap<Integer, HashSet<Integer>> repVertexBelongsTo;
    private final HashSet<Integer> splitVertices;
    private final Graph graph;
    private final List<Integer> clusterList;
    private final int maxVolume;

    public StreamCluster(Graph graph) {
        this.cluster = new int[GlobalConfig.vCount];
        this.graph = graph;
        this.volume = new HashMap<>();
        this.maxVolume = GlobalConfig.getMaxClusterVolume();
        this.innerAndCutEdge = new HashMap<>();
        this.clusterList = new ArrayList<>();
        this.degree = new int[GlobalConfig.vCount];
        this.repVertexBelongsTo = new HashMap<>();
        this.splitVertices = new HashSet<>();
    }

    private void combineCluster(int srcVid, int destVid) {

        int minVid = (volume.get(cluster[srcVid]) < volume.get(cluster[destVid]) ? srcVid : destVid);
        int maxVid = (srcVid == minVid ? destVid : srcVid);

        volume.put(cluster[maxVid], volume.get(cluster[maxVid]) + this.degree[minVid]);
        volume.put(cluster[minVid], volume.get(cluster[minVid]) - this.degree[minVid]);
        if (volume.get(cluster[minVid]) == 0) volume.remove(cluster[minVid]);

        cluster[minVid] = cluster[maxVid];
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

            this.degree[src]++;
            this.degree[dest]++;

            // update volume
            if (!volume.containsKey(cluster[src])) {
                volume.put(cluster[src], 0);
            }
            if (!volume.containsKey(cluster[dest])) {
                volume.put(cluster[dest], 0);
            }
            volume.put(cluster[src], volume.get(cluster[src]) + 1);
            volume.put(cluster[dest], volume.get(cluster[dest]) + 1);

            if (volume.get(cluster[src]) >= maxVolume) {
                volume.put(cluster[src], volume.get(cluster[src]) - this.degree[src]);
                cluster[src] = clusterID++;
                splitVertices.add(src);
                volume.put(cluster[src], this.degree[src]);
            }

            if (volume.get(cluster[dest]) >= maxVolume) {
                volume.put(cluster[dest], volume.get(cluster[dest]) - this.degree[dest]);
                cluster[dest] = clusterID++;
                splitVertices.add(dest);
                volume.put(cluster[dest], this.degree[dest]);
            }

            // combine cluster
            combineCluster(src, dest);
        }
        setUpIndex();
        computeEdgeInfo();
    }

    private void setUpIndex() {
        // sort the volume of the cluster
        List<HashMap.Entry<Integer, Integer>> sortList = new ArrayList<HashMap.Entry<Integer, Integer>>(volume.entrySet());
        for (int i = 0; i < sortList.size(); i++) {
            this.clusterList.add(sortList.get(i).getKey());
        }
        volume.clear();
        System.gc();
    }

    private void computeEdgeInfo() {
        // compute inner and cut edge
        graph.readGraphFromFile();

        Edge edge;
        while ((edge = graph.readStep()) != null) {

            int src = edge.getSrcVId();
            int dest = edge.getDestVId();

            if (!innerAndCutEdge.containsKey(cluster[src]))
                innerAndCutEdge.put(cluster[src], new HashMap<>());

            if (!innerAndCutEdge.get(cluster[src]).containsKey(cluster[src]))
                innerAndCutEdge.get(cluster[src]).put(cluster[src], 0);

            if (!innerAndCutEdge.containsKey(cluster[dest]))
                innerAndCutEdge.put(cluster[dest], new HashMap<>());

            if (!innerAndCutEdge.get(cluster[dest]).containsKey(cluster[dest]))
                innerAndCutEdge.get(cluster[dest]).put(cluster[dest], 0);

            if (!innerAndCutEdge.get(cluster[src]).containsKey(cluster[dest]))
                innerAndCutEdge.get(cluster[src]).put(cluster[dest], 0);

            if (cluster[src] == cluster[dest]) {
                int oldValue = innerAndCutEdge.get(cluster[src]).get(cluster[src]);
                innerAndCutEdge.get(cluster[src]).put(cluster[src], oldValue + 1);
                continue;
            }

            if (splitVertices.contains(src) && splitVertices.contains(dest)) {
                int minVid = degree[src] < degree[dest] ? src : dest;
                int maxVid = (src == minVid ? dest : src);

                if (!repVertexBelongsTo.containsKey(maxVid))
                    repVertexBelongsTo.put(maxVid, new HashSet<>());

                repVertexBelongsTo.get(maxVid).add(cluster[maxVid]);
                repVertexBelongsTo.get(maxVid).add(cluster[minVid]);
            } else {

                int splitId, notSplitId;

                if (splitVertices.contains(src)) splitId = src;
                else if (splitVertices.contains(dest)) splitId = dest;
                else if (degree[src] > degree[dest]) splitId = src;
                else splitId = dest;

                notSplitId = (splitId == src ? dest : src);

                if (!repVertexBelongsTo.containsKey(splitId))
                    repVertexBelongsTo.put(splitId, new HashSet<>());

                repVertexBelongsTo.get(splitId).add(cluster[splitId]);
                repVertexBelongsTo.get(splitId).add(cluster[notSplitId]);
            }
        }

        for (Map.Entry<Integer, HashSet<Integer>> entry : this.repVertexBelongsTo.entrySet()) {

            HashSet<Integer> virtualSet = entry.getValue();

            Object[] arr = virtualSet.toArray();

            for (int i = 0; i < arr.length; i++) {
                for (int j = i + 1; j < arr.length; j++) {
                    if (!innerAndCutEdge.containsKey(arr[i])) {
                        innerAndCutEdge.put((Integer) arr[i], new HashMap<>());
                    }

                    if (!innerAndCutEdge.get(arr[i]).containsKey(arr[j])) {
                        innerAndCutEdge.get(arr[i]).put((Integer) arr[j], 0);
                    }

                    int oldValue = innerAndCutEdge.get(arr[i]).get(arr[j]);
                    innerAndCutEdge.get(arr[i]).put((Integer) arr[j], oldValue + 1);
                }
            }

        }

    }

    public List<Integer> getClusterList() {
        return clusterList;
    }

    public HashMap<Integer, HashMap<Integer, Integer>> getInnerAndCutEdge() {
        return innerAndCutEdge;
    }

    public int getEdgeNum(int cluster1, int cluster2) {
        if (!innerAndCutEdge.containsKey(cluster1) || !innerAndCutEdge.get(cluster1).containsKey(cluster2)) return 0;

        return innerAndCutEdge.get(cluster1).get(cluster2);
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

    public int getClusterId(int vId) {
        return cluster[vId];
    }

    public HashMap<Integer, Integer> getVolume() {
        return volume;
    }
}
