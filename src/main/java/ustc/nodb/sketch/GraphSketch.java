package ustc.nodb.sketch;

import com.google.common.hash.Hashing;
import ustc.nodb.core.Edge;
import ustc.nodb.core.Graph;
import ustc.nodb.properties.GlobalConfig;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.TreeSet;

public class GraphSketch {

    private final int vCount;
    private final int[][] adjMatrix;
    private final ArrayList<HashSet<Integer>> vertexHashTable;
    private final Graph graph;
    private final int[] degree;
    private final int[][] hashFunc = {
            {13, 17},
            {17, 23},
            {19, 29},
            {31, 17},
            {23, 31},
            {47, 41},
            {41, 13},
            {59, 7},
            {53, 29},
            {29, 43},
            {43, 19},
    };

    public GraphSketch(Graph graph) {
        this.vCount = (short) Math.round(Math.sqrt(graph.getECount() / GlobalConfig.getCompressionRate()));
        adjMatrix = new int[this.vCount][this.vCount];
        vertexHashTable = new ArrayList<>(this.vCount);
        for (int i = 0; i < this.vCount; i++) {
            vertexHashTable.add(new HashSet<Integer>());
        }
        this.graph = graph;
        degree = new int[this.vCount];
    }

    public void setupAdjMatrix(int hashFuncIndex) {
        for (Edge edge : graph.getEdgeList()) {
            int src = edge.getSrcVId();
            int dest = edge.getDestVId();
            int srcHash = Hashing.sha256().hashInt(src).hashCode();
            srcHash = Math.floorMod(srcHash * this.hashFunc[hashFuncIndex][0] + this.hashFunc[hashFuncIndex][1], this.vCount);
            int destHash = Hashing.sha256().hashInt(dest).hashCode();
            destHash = Math.floorMod(destHash * this.hashFunc[hashFuncIndex][0] + this.hashFunc[hashFuncIndex][1], this.vCount);

            vertexHashTable.get(srcHash).add(src);
            vertexHashTable.get(destHash).add(dest);
            adjMatrix[srcHash][destHash]++;
            degree[srcHash]++;
            if(srcHash != destHash) degree[destHash]++;
        }
    }

    @Override
    public String toString() {
        StringBuilder ajdMatrixStr = new StringBuilder();
        StringBuilder hashTableStr = new StringBuilder();
        StringBuilder degreeStr = new StringBuilder();

        ajdMatrixStr.append("Matrix: \n");
        for (int i = 0; i < this.vCount; i++) {
            for (int j = 0; j < this.vCount; j++) {
                ajdMatrixStr.append(" ").append(adjMatrix[i][j]);
            }
            ajdMatrixStr.append("\n");
        }

        hashTableStr.append("HashTable: \n");
        for (int i = 0; i < this.vCount; i++) {
            HashSet<Integer> vSet = vertexHashTable.get(i);
            hashTableStr.append(i).append(":");
            for (Integer v : vSet) {
                hashTableStr.append(" ").append(v);
            }
            hashTableStr.append("\n");
        }

        degreeStr.append("Degree: \n");
        for (int i = 0; i < this.vCount; i++) {
            degreeStr.append(i).append(": ").append(degree[i]);
            degreeStr.append("\n");
        }

        return ajdMatrixStr.toString() + hashTableStr.toString() + degreeStr.toString();
    }

    public int[][] getAdjMatrix() {
        return adjMatrix;
    }

    public int getVCount() {
        return vCount;
    }

    public int getDegree(int vid) {
        return degree[vid];
    }

    public int findWeight(int i, int j){
        return adjMatrix[i][j];
    }
}
