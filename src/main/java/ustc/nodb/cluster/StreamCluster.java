package ustc.nodb.cluster;

import ustc.nodb.Graph.Graph;
import ustc.nodb.core.Edge;
import ustc.nodb.properties.GlobalConfig;
import ustc.nodb.Graph.SketchGraph;

import java.util.*;

public class StreamCluster {

    private final int[] cluster;
    private final HashMap<Integer, Integer> volume;
    // clusterId1 = clusterId2 save inner, otherwise save cut
    private final HashMap<Integer, HashMap<Integer, Integer>> innerAndCutEdge;
    private final Graph graph;
    private final ArrayList<Integer> clusterList;
    private final int maxVolume;

    public StreamCluster(Graph graph) {
        this.cluster = new int[graph.getVCount()];
        this.graph = graph;
        this.volume = new HashMap<>();
        this.maxVolume = GlobalConfig.getMaxClusterVolume();
        this.innerAndCutEdge = new HashMap<>();
        this.clusterList = new ArrayList<>();
    }

    private void combineCluster(int srcVid, int destVid) {
        if (volume.get(cluster[srcVid]) >= maxVolume || volume.get(cluster[destVid]) >= maxVolume) return;

        int minVid = (volume.get(cluster[srcVid]) < volume.get(cluster[destVid]) ? srcVid : destVid);
        int maxVid = (srcVid == minVid ? destVid : srcVid);

        if ((volume.get(cluster[maxVid]) + graph.getDegree(minVid)) <= maxVolume) {
            volume.put(cluster[maxVid], volume.get(cluster[maxVid]) + graph.getDegree(minVid));
            volume.put(cluster[minVid], volume.get(cluster[minVid]) - graph.getDegree(minVid));
            if (volume.get(cluster[minVid]) == 0) volume.remove(cluster[minVid]);
            cluster[minVid] = cluster[maxVid];
        }
    }

    public void startSteamCluster() {

        int clusterID = 1;

        for (Edge edge : graph.getEdgeList()) {

            int src = edge.getSrcVId();
            int dest = edge.getDestVId();

            // allocate cluster
            if (cluster[src] == 0) cluster[src] = clusterID++;
            if (cluster[dest] == 0) cluster[dest] = clusterID++;

            // update volume
            if (!volume.containsKey(cluster[src])) {
                volume.put(cluster[src], graph.getDegree(src));
            }
            if (!volume.containsKey(cluster[dest])) {
                volume.put(cluster[dest], graph.getDegree(dest));
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
//        Collections.sort(sortList, (v1, v2) -> (v2.getValue() - v1.getValue()));
        for(int i = 0; i < sortList.size(); i++){
            this.clusterList.add(sortList.get(i).getKey());
        }
    }

    private void computeEdgeInfo() {
        // compute inner and cut edge
        for (Edge edge : graph.getEdgeList()) {

            int src = edge.getSrcVId();
            int dest = edge.getDestVId();

            if (!innerAndCutEdge.containsKey(cluster[src]))
                innerAndCutEdge.put(cluster[src], new HashMap<>());

            if (!innerAndCutEdge.get(cluster[src]).containsKey(cluster[dest]))
                innerAndCutEdge.get(cluster[src]).put(cluster[dest], 0);

            int oldValue = innerAndCutEdge.get(cluster[src]).get(cluster[dest]);
            innerAndCutEdge.get(cluster[src]).put(cluster[dest], oldValue + edge.getWeight());
        }
    }

    public ArrayList<Integer> getClusterList() {
        return clusterList;
    }

    public HashMap<Integer, HashMap<Integer, Integer>> getInnerAndCutEdge() {
        return innerAndCutEdge;
    }

    public int getEdgeNum(int cluster1, int cluster2) {
        if(!innerAndCutEdge.containsKey(cluster1) || !innerAndCutEdge.get(cluster1).containsKey(cluster2)) return 0;

        return innerAndCutEdge.get(cluster1).get(cluster2);
    }

    @Override
    public String toString() {
        StringBuilder volumeStr = new StringBuilder();
        StringBuilder clusterStr = new StringBuilder();
        volume.forEach((k, v) -> {
            volumeStr.append("cluster ").append(k).append(" volume: ").append(v).append("\n");
        });

        for (int i = 0; i < graph.getVCount(); i++) {
            clusterStr.append("vid : ").append(i).append(" cluster: ").append(cluster[i]).append("\n");
        }

        return volumeStr.toString() + clusterStr.toString();
    }

    public int getClusterId(int vId) {
        return cluster[vId];
    }

    public HashMap<Integer, Integer> getVolume() {
        return volume;
    }
}
