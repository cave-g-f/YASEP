package ustc.nodb.cluster;

import ustc.nodb.Graph.*;
import ustc.nodb.IO.*;
import ustc.nodb.properties.*;
import ustc.nodb.core.*;
import java.util.*;

public class EdgeStreamCluster
{
    private final HashMap<Integer, Integer> cluster;
    private final HashMap<Integer, Integer> degree;
    private final HashMap<Integer, Integer> volume;
    private final HashMap<Integer, Integer> clusterReorder;
    private final HashMap<Integer, HashSet<Integer>> batchVertex;
    private final HashMap<Integer, HashSet<Integer>> vertexEachCluster;
    private final HashMap<Integer, Integer> batchEdge;
    private final Graph graph;
    private final ArrayList<Integer> clusterList;
    private final HashMap<Integer, ArrayList<Integer>> writeBuffer;
    private final HashMap<Integer, ArrayList<Integer>> resultBuffer;
    private HashMap<Integer, ArrayList<Integer>> buffer;
    private final int maxVolume;
    private final int batchsize;
    private final BatchFileIO batchFileIO;
    private final HashMap<Integer, HashSet<Integer>> repVertexEachCluster;
    private final HashMap<Integer, HashSet<Integer>> repVertexBelongsTo;
    
    public EdgeStreamCluster(final Graph graph, final BatchFileIO batchFileIO) {
        this.cluster = new HashMap<Integer, Integer>();
        this.graph = graph;
        this.volume = new HashMap<Integer, Integer>();
        this.maxVolume = GlobalConfig.eCount / GlobalConfig.getClusterNumber() + 1;
        this.clusterList = new ArrayList<Integer>();
        this.degree = new HashMap<Integer, Integer>();
        this.batchsize = GlobalConfig.batchSize;
        this.clusterReorder = new HashMap<Integer, Integer>();
        this.writeBuffer = new HashMap<Integer, ArrayList<Integer>>();
        this.resultBuffer = new HashMap<Integer, ArrayList<Integer>>();
        this.buffer = this.writeBuffer;
        this.batchFileIO = batchFileIO;
        this.repVertexEachCluster = new HashMap<Integer, HashSet<Integer>>();
        this.repVertexBelongsTo = new HashMap<Integer, HashSet<Integer>>();
        this.batchVertex = new HashMap<Integer, HashSet<Integer>>();
        this.batchEdge = new HashMap<Integer, Integer>();
        this.vertexEachCluster = new HashMap<Integer, HashSet<Integer>>();
    }
    
    private void combineCluster(final int srcVid, final int destVid) {
        if (this.volume.get(this.cluster.get(srcVid)) >= this.maxVolume || this.volume.get(this.cluster.get(destVid)) >= this.maxVolume) {
            return;
        }
        final int minVid = (this.volume.get(this.cluster.get(srcVid)) < this.volume.get(this.cluster.get(destVid))) ? srcVid : destVid;
        final int maxVid = (srcVid == minVid) ? destVid : srcVid;
        if (this.volume.get(this.cluster.get(maxVid)) + 1 <= this.maxVolume) {
            this.volume.put(this.cluster.get(maxVid), this.volume.get(this.cluster.get(maxVid)) + 1);
            if (this.volume.get(this.cluster.get(minVid)) == 0) {
                this.volume.remove(this.cluster.get(minVid));
            }
            else {
                if (!this.repVertexBelongsTo.containsKey(minVid)) {
                    this.repVertexBelongsTo.put(minVid, new HashSet<Integer>());
                }
                if (!this.repVertexEachCluster.containsKey(this.cluster.get(minVid))) {
                    this.repVertexEachCluster.put(this.cluster.get(minVid), new HashSet<Integer>());
                }
                if (!this.repVertexEachCluster.containsKey(this.cluster.get(maxVid))) {
                    this.repVertexEachCluster.put(this.cluster.get(maxVid), new HashSet<Integer>());
                }
                this.repVertexEachCluster.get(this.cluster.get(minVid)).add(minVid);
                this.repVertexEachCluster.get(this.cluster.get(maxVid)).add(minVid);
                this.repVertexBelongsTo.get(minVid).add(this.cluster.get(minVid));
                this.repVertexBelongsTo.get(minVid).add(this.cluster.get(maxVid));
            }
            this.cluster.put(minVid, this.cluster.get(maxVid));
        }
    }
    
    public void startSteamCluster() {
        int nextClusterID = 1;
        int clusterNum = 0;
        int processedEdge = 0;
        this.graph.readGraphFromFile();
        Edge edge;
        while ((edge = this.graph.readStep()) != null) {
            final int src = edge.getSrcVId();
            final int dest = edge.getDestVId();
            if (!this.cluster.containsKey(src)) {
                this.cluster.put(src, nextClusterID++);
            }
            if (!this.cluster.containsKey(dest)) {
                this.cluster.put(dest, nextClusterID++);
            }
            if (!this.volume.containsKey(this.cluster.get(src))) {
                this.volume.put(this.cluster.get(src), 0);
            }
            if (!this.volume.containsKey(this.cluster.get(dest))) {
                this.volume.put(this.cluster.get(dest), 0);
            }
            if (this.volume.get(this.cluster.get(src)) >= this.maxVolume) {
                if (!this.repVertexBelongsTo.containsKey(src)) {
                    this.repVertexBelongsTo.put(src, new HashSet<Integer>());
                }
                if (!this.repVertexEachCluster.containsKey(this.cluster.get(src))) {
                    this.repVertexEachCluster.put(this.cluster.get(src), new HashSet<Integer>());
                }
                this.repVertexEachCluster.get(this.cluster.get(src)).add(src);
                this.repVertexBelongsTo.get(src).add(this.cluster.get(src));
                this.cluster.put(src, nextClusterID++);
                this.volume.put(this.cluster.get(src), 0);
            }
            if (this.volume.get(this.cluster.get(dest)) >= this.maxVolume) {
                if (!this.repVertexBelongsTo.containsKey(dest)) {
                    this.repVertexBelongsTo.put(dest, new HashSet<Integer>());
                }
                if (!this.repVertexEachCluster.containsKey(this.cluster.get(dest))) {
                    this.repVertexEachCluster.put(this.cluster.get(dest), new HashSet<Integer>());
                }
                this.repVertexEachCluster.get(this.cluster.get(dest)).add(dest);
                this.repVertexBelongsTo.get(dest).add(this.cluster.get(dest));
                this.cluster.put(dest, nextClusterID++);
                this.volume.put(this.cluster.get(dest), 0);
            }
            this.combineCluster(src, dest);
            if (!this.vertexEachCluster.containsKey(this.cluster.get(src))) {
                this.vertexEachCluster.put(this.cluster.get(src), new HashSet<Integer>());
                this.vertexEachCluster.get(this.cluster.get(src)).add(src);
                this.vertexEachCluster.get(this.cluster.get(src)).add(dest);
            }
            if (!this.clusterReorder.containsKey(this.cluster.get(src))) {
                this.clusterList.add(this.cluster.get(src));
                this.clusterReorder.put(this.cluster.get(src), clusterNum++);
            }
            final int batchID = this.clusterReorder.get(this.cluster.get(src)) / this.batchsize;
            if (!this.batchVertex.containsKey(batchID)) {
                this.batchVertex.put(batchID, new HashSet<Integer>());
            }
            if (!this.batchEdge.containsKey(batchID)) {
                this.batchEdge.put(batchID, 0);
            }
            if (!this.buffer.containsKey(batchID)) {
                this.buffer.put(batchID, new ArrayList<Integer>());
            }
            this.buffer.get(batchID).add(src);
            this.buffer.get(batchID).add(dest);
            this.buffer.get(batchID).add(this.cluster.get(src));
            this.batchVertex.get(batchID).add(src);
            this.batchVertex.get(batchID).add(dest);
            this.batchEdge.put(batchID, this.batchEdge.get(batchID) + 1);
            if (++processedEdge % GlobalConfig.vCount == 0) {
                processedEdge = 0;
                while (!this.batchFileIO.isDone()) {}
                this.batchFileIO.outputFilesAsyn((HashMap)this.buffer);
                if (this.buffer == this.writeBuffer) {
                    this.resultBuffer.clear();
                    this.buffer = this.resultBuffer;
                }
                else {
                    this.writeBuffer.clear();
                    this.buffer = this.writeBuffer;
                }
            }
        }
        if (processedEdge != 0) {
            processedEdge = 0;
            while (!this.batchFileIO.isDone()) {}
            this.batchFileIO.outputFilesAsyn((HashMap)this.buffer);
        }
        this.batchFileIO.closeOutputAsyn();
    }
    
    private void showInfo() {
        int totalVolume = 0;
        for (int i = 0; i < this.clusterList.size(); ++i) {
            System.out.println("cluster size: " + this.volume.get(this.clusterList.get(i)).toString());
            totalVolume += this.volume.get(this.clusterList.get(i));
        }
        System.out.println("total volume: " + totalVolume);
        System.out.println("total cluster: " + this.volume.size());
        int totalRep = 0;
        int totalRepInv = 0;
        for (final Map.Entry<Integer, HashSet<Integer>> entry : this.repVertexEachCluster.entrySet()) {
            totalRep += entry.getValue().size();
        }
        for (final Map.Entry<Integer, HashSet<Integer>> entry : this.repVertexBelongsTo.entrySet()) {
            totalRepInv += entry.getValue().size();
        }
        System.out.println("totalRep: " + totalRep + " totalRepInv: " + totalRepInv);
        System.out.println("the number of replicated vertex: " + this.repVertexBelongsTo.size());
        int totalBatchEdge = 0;
        for (final Map.Entry<Integer, Integer> entry2 : this.batchEdge.entrySet()) {
            totalBatchEdge += entry2.getValue();
        }
        System.out.println("batch edge sum: " + totalBatchEdge);
    }
    
    public int getVolume(final int clusterID) {
        return this.volume.get(clusterID);
    }
    
    public HashSet<Integer> getRepVertexEachCluster(final int clusterID) {
        if (!this.repVertexEachCluster.containsKey(clusterID)) {
            return new HashSet<Integer>();
        }
        return this.repVertexEachCluster.get(clusterID);
    }
    
    public HashSet<Integer> getRepVertexBelongsTo(final int vertexID) {
        return this.repVertexBelongsTo.get(vertexID);
    }
    
    public int getBatchVertex(final int batchID) {
        return this.batchVertex.get(batchID).size();
    }
    
    public int getBatchEdge(final int batchID) {
        return this.batchEdge.get(batchID);
    }
    
    public int getClusterVertex(final int clusterID) {
        return this.vertexEachCluster.get(clusterID).size();
    }
    
    public ArrayList<Integer> getClusterList() {
        return this.clusterList;
    }
    
    public int getBatchNum() {
        return this.batchEdge.size();
    }
}
