package ustc.nodb.cluster;

import com.sun.org.apache.xerces.internal.xinclude.XIncludeNamespaceSupport;
import org.checkerframework.checker.units.qual.A;
import org.checkerframework.checker.units.qual.C;
import org.graalvm.compiler.hotspot.replacements.HashCodeSnippets;
import ustc.nodb.Graph.Graph;
import ustc.nodb.IO.BatchFileIO;
import ustc.nodb.core.Edge;
import ustc.nodb.game.ClusterPackEdgeGame;
import ustc.nodb.properties.GlobalConfig;

import java.nio.ByteBuffer;
import java.util.*;

public class EdgeStreamCluster {

    private final HashMap<Integer, HashSet<Cluster>> cluster;
    private final HashMap<Integer, Integer> degree;
    private final HashMap<Integer, HashSet<Integer>> batchVertex;
    private final HashMap<Integer, Integer> batchEdge;
    private final Graph graph;
    private final ArrayList<Cluster> clusterList;
    private final HashMap<Integer, ArrayList<Integer>> writeBuffer;
    private final HashMap<Integer, ArrayList<Integer>> resultBuffer;
    private HashMap<Integer, ArrayList<Integer>> buffer;
    private final int maxVolume;
    private final int batchsize;
    private final BatchFileIO batchFileIO;
    private int processedEdge;

    public EdgeStreamCluster(Graph graph, BatchFileIO batchFileIO) {
        this.cluster = new HashMap<>();
        this.graph = graph;
        this.maxVolume = GlobalConfig.eCount / GlobalConfig.getClusterNumber() + 1;
        this.clusterList = new ArrayList<>();
        this.degree = new HashMap<>();
        this.batchsize = GlobalConfig.batchSize;
        this.writeBuffer  = new HashMap<>();
        this.resultBuffer  = new HashMap<>();
        this.buffer = writeBuffer;
        this.batchFileIO = batchFileIO;
        this.batchVertex = new HashMap<>();
        this.batchEdge = new HashMap<>();
        this.processedEdge = 0;
    }

    private int checkNewStateEdge(Edge edge, int nextId){
        int src = edge.getSrcVId();
        int dest = edge.getDestVId();

        if(!cluster.containsKey(src) && !cluster.containsKey(dest)) {
            Cluster c = new Cluster(nextId);
            c.volume = 1;
            cluster.put(src, new HashSet<>());
            cluster.put(dest, new HashSet<>());
            clusterList.add(c);
            cluster.get(src).add(c);
            cluster.get(dest).add(c);

            return nextId;
        }

        return -1;
    }

    private int checkReadyStateEdge(Edge edge){
        int src = edge.getSrcVId();
        int dest = edge.getDestVId();

        PriorityQueue<Cluster> pq = new PriorityQueue<>((c1, c2) -> c2.volume - c1.volume);
        pq.addAll(cluster.get(src));
        pq.addAll(cluster.get(dest));

        while(!pq.isEmpty()){
            Cluster target = pq.peek();
            if(target.volume < maxVolume){

                if(!cluster.containsKey(src)) cluster.put(src, new HashSet<>());
                if(!cluster.containsKey(dest)) cluster.put(dest, new HashSet<>());

                // assign edge
                cluster.get(src).add(target);
                cluster.get(dest).add(target);
                target.volume += 1;

                return target.id;
            }
            pq.remove(target);
        }

        return -1;
    }

    private int checkDriftStateEdge(Edge edge, int nextId){
        int src = edge.getSrcVId();
        int dest = edge.getDestVId();

        if(!cluster.containsKey(src)) cluster.put(src, new HashSet<>());
        if(!cluster.containsKey(dest)) cluster.put(dest, new HashSet<>());

        Cluster c = new Cluster(nextId);
        c.volume = 1;
        cluster.put(src, new HashSet<>());
        cluster.put(dest, new HashSet<>());
        clusterList.add(c);
        cluster.get(src).add(c);
        cluster.get(dest).add(c);

        return nextId;
    }

    private void persistResult(int target, Edge edge){

        int src = edge.getSrcVId();
        int dest = edge.getDestVId();

        // put edge into write buffer, store format (src dest clusterID)
        int batchID = target / batchsize;
        if (!batchVertex.containsKey(batchID)){
            batchVertex.put(batchID, new HashSet<>());
        }
        if (!batchEdge.containsKey(batchID)){
            batchEdge.put(batchID, 0);
        }
        if (!buffer.containsKey(batchID)){
            buffer.put(batchID, new ArrayList<Integer>());
        }
        buffer.get(batchID).add(src);
        buffer.get(batchID).add(dest);
        buffer.get(batchID).add(target);
        batchVertex.get(batchID).add(src);
        batchVertex.get(batchID).add(dest);
        batchEdge.put(batchID, batchEdge.get(batchID) + 1);
        processedEdge += 1;
        if (processedEdge % GlobalConfig.vCount == 0){
            processedEdge = 0;

            while(!this.batchFileIO.isDone());
            this.batchFileIO.outputFilesAsyn(buffer);

            if(buffer == writeBuffer) {
                resultBuffer.clear();
                buffer = resultBuffer;
            }
            else {
                writeBuffer.clear();
                buffer = writeBuffer;
            }
        }

    }

    public void startSteamCluster() {

        int nextClusterID = 0;
        int clusterNum = 0;

        graph.readGraphFromFile();

        Edge edge;
        while ((edge = graph.readStep()) != null) {

            int target = 0;

            if((target = checkNewStateEdge(edge, nextClusterID)) != -1){
                persistResult(target, edge);
                nextClusterID++;
                continue;
            }

            if((target = checkReadyStateEdge(edge)) != -1){
                persistResult(target, edge);
                continue;
            }

            if((target = checkDriftStateEdge(edge, nextClusterID)) != -1){
                persistResult(target, edge);
                nextClusterID++;
                continue;
            }

        }
        if (processedEdge != 0){
            processedEdge = 0;
            while(!this.batchFileIO.isDone());
            this.batchFileIO.outputFilesAsyn(buffer);
        }

        // update rep vertex
        for(Map.Entry<Integer, HashSet<Cluster>> v2c : cluster.entrySet()){
            if(v2c.getValue().size() <= 1) continue;
            for(Cluster c : v2c.getValue()){
                c.repVertex.add(v2c.getKey());
            }
        }

        showInfo();
        this.batchFileIO.closeOutputAsyn();
        this.batchFileIO.checkBatchFile();
    }

    private void showInfo() {

        int totalVolume = 0;
        for (int i = 0; i < this.clusterList.size(); i++) {
            totalVolume += clusterList.get(i).volume;
        }
        System.out.println("total volume: " + totalVolume);
        System.out.println("total cluster: " + clusterList.size());

        // check rep statistic
        int totalRep = 0;
        int totalRepInv = 0;
        for(Cluster c : clusterList){
            totalRep += c.repVertex.size();
        }
        System.out.println("totalRep: " + totalRep);

        // check batch info
        int totalBatchEdge = 0;
        for(Map.Entry<Integer, Integer> entry : this.batchEdge.entrySet()){
            totalBatchEdge += entry.getValue();
        }
        System.out.println("batch edge sum: " + totalBatchEdge);

    }

    public int getBatchVertex(int batchID){
        return batchVertex.get(batchID).size();
    }

    public int getBatchEdge(int batchID){
        return batchEdge.get(batchID);
    }


    public ArrayList<Cluster> getClusterList(){
        return this.clusterList;
    }

    public int getBatchNum(){
        return this.batchEdge.size();
    }
}
