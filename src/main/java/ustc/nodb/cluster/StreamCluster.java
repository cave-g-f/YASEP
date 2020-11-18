package ustc.nodb.cluster;

import ustc.nodb.properties.GlobalConfig;
import ustc.nodb.sketch.GraphSketch;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Iterator;

public class StreamCluster {

    private final int[] cluster;
    private final HashMap<Integer, Integer> volume;
    // clusterId1 = clusterId2 save inner, otherwise save cut
    private final HashMap<Integer, HashMap<Integer, Integer>> innerAndCutEdge;
    private final GraphSketch sketch;
    private int[] clusterId;
    private int clusterNum;
    private final int maxVolume;

    public StreamCluster(GraphSketch sketch) {
        this.cluster = new int[sketch.getVCount()];
        this.volume = new HashMap<>();
        this.sketch = sketch;
        this.maxVolume = GlobalConfig.getMaxClusterVolume();
        this.innerAndCutEdge = new HashMap<>();
    }

    private void combineCluster(int srcVid, int destVid) {
        if (volume.get(cluster[srcVid]) >= maxVolume || volume.get(cluster[destVid]) >= maxVolume) return;

        int minVid = (volume.get(cluster[srcVid]) < volume.get(cluster[destVid]) ? srcVid : destVid);
        int maxVid = (srcVid == minVid ? destVid : srcVid);

        if ((volume.get(cluster[maxVid]) + sketch.getDegree(minVid)) <= maxVolume) {
            volume.put(cluster[maxVid], volume.get(cluster[maxVid]) + sketch.getDegree(minVid));
            volume.put(cluster[minVid], volume.get(cluster[minVid]) - sketch.getDegree(minVid));
            if (volume.get(cluster[minVid]) == 0) volume.remove(cluster[minVid]);
            cluster[minVid] = cluster[maxVid];
        }
    }

    public void startSteamCluster() {

        int clusterID = 1;

        for (int i = 0; i < sketch.getVCount(); i++) {
            for (int j = i + 1; j < sketch.getVCount(); j++) {

                if (sketch.findWeight(i, j) == 0) continue;

                // allocate cluster
                if (cluster[i] == 0) cluster[i] = clusterID++;
                if (cluster[j] == 0) cluster[j] = clusterID++;

                // update volume
                if (!volume.containsKey(cluster[i])) {
                    volume.put(cluster[i], sketch.getDegree(i));
                }
                if (!volume.containsKey(cluster[j])) {
                    volume.put(cluster[j], sketch.getDegree(j));
                }

                // combine cluster
                combineCluster(i, j);
            }
        }
        setUpIndex();
        computeEdgeInfo();
    }

    private void setUpIndex() {
        // set cluster id index
        clusterNum = volume.size();
        clusterId = new int[volume.size()];
        Iterator<Integer> iterator = volume.keySet().iterator();
        int i = 0;
        while (iterator.hasNext()) {
            clusterId[i] = iterator.next();
        }
    }

    private void computeEdgeInfo() {
        // compute inner and cut edge
        for(int i = 0; i < sketch.getVCount(); i++){
            for(int j = 0; j < sketch.getVCount(); j++){
                if(sketch.findWeight(i, j) == 0) continue;

                if(!innerAndCutEdge.containsKey(cluster[i]))
                    innerAndCutEdge.put(cluster[i], new HashMap<>());

                if(!innerAndCutEdge.get(cluster[i]).containsKey(cluster[j]))
                    innerAndCutEdge.get(cluster[i]).put(cluster[j], 0);

                int oldValue = innerAndCutEdge.get(cluster[i]).get(cluster[j]);
                innerAndCutEdge.get(cluster[i]).put(cluster[j], oldValue + sketch.findWeight(i, j));
            }
        }
    }

    public int getClusterNum() {
        return clusterNum;
    }

    public HashMap<Integer, HashMap<Integer, Integer>> getInnerAndCutEdge() {
        return innerAndCutEdge;
    }

    @Override
    public String toString() {
        StringBuilder volumeStr = new StringBuilder();
        StringBuilder clusterStr = new StringBuilder();
        volume.forEach((k, v) -> {
            volumeStr.append("cluster ").append(k).append(" volume: ").append(v).append("\n");
        });

        for (int i = 0; i < sketch.getVCount(); i++) {
            clusterStr.append("vid : ").append(i).append(" cluster: ").append(cluster[i]).append("\n");
        }

        return volumeStr.toString() + clusterStr.toString();
    }
}
