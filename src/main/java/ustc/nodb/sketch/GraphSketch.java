package ustc.nodb.sketch;

import com.google.common.hash.Hashing;
import ustc.nodb.core.Edge;
import ustc.nodb.core.Graph;
import ustc.nodb.properties.GlobalConfig;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashSet;

public class GraphSketch {

    private final ArrayList<Edge> edgeList;
    private final int vCount;
    private final int eCount;
    private final byte[][] adjMatrix;
    private final short hashSize;
    private final ArrayList<HashSet<Integer>> vertexHashTable;
    private final Graph graph;
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
        this.vCount = graph.getVCount();
        this.eCount = graph.getECount();
        this.hashSize = (short) Math.round(Math.sqrt(this.eCount / GlobalConfig.getCompressionRate()));
        adjMatrix = new byte[hashSize][hashSize];
        vertexHashTable = new ArrayList<>(hashSize);
        for (int i = 0; i < hashSize; i++) {
            vertexHashTable.add(new HashSet<Integer>());
        }
        this.graph = graph;
        this.edgeList = graph.getEdgeList();
    }

    public void setupAdjMatrix(int hashFuncIndex) {
        for (Edge edge : edgeList) {
            int src = edge.getSrcVId();
            int dest = edge.getDestVId();
            int srcHash = Hashing.sha256().hashInt(src).hashCode();
            srcHash = Math.floorMod(srcHash * this.hashFunc[hashFuncIndex][0] + this.hashFunc[hashFuncIndex][1], this.hashSize);
            int destHash = Hashing.sha256().hashInt(dest).hashCode();
            destHash = Math.floorMod(destHash * this.hashFunc[hashFuncIndex][0] + this.hashFunc[hashFuncIndex][1], this.hashSize);

            vertexHashTable.get(srcHash).add(src);
            vertexHashTable.get(destHash).add(dest);
            adjMatrix[srcHash][destHash]++;
        }
    }

    @Override
    public String toString() {
        StringBuilder ajdMatrixStr = new StringBuilder();
        StringBuilder hashTableStr = new StringBuilder();

        ajdMatrixStr.append("Matrix: \n");
        for (int i = 0; i < hashSize; i++) {
            for (int j = 0; j < hashSize; j++) {
                ajdMatrixStr.append(" ").append(adjMatrix[i][j]);
            }
            ajdMatrixStr.append("\n");
        }

        hashTableStr.append("HashTable: \n");
        for (int i = 0; i < hashSize; i++) {
            HashSet<Integer> vSet = vertexHashTable.get(i);
            hashTableStr.append(i).append(":");
            for (Integer v : vSet) {
                hashTableStr.append(" ").append(v);
            }
            hashTableStr.append("\n");
        }

        return ajdMatrixStr.toString() + hashTableStr.toString();
    }
}
