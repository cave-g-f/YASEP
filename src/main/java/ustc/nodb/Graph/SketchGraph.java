package ustc.nodb.Graph;

import com.google.common.hash.Hashing;
import ustc.nodb.core.Edge;
import ustc.nodb.properties.GlobalConfig;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.HashSet;

public class SketchGraph implements Graph{

    private final int vCount;
    private final int[][] adjMatrix;
    private final ArrayList<Edge> edgeList;
    private final ArrayList<HashSet<Integer>> vertexHashTable;
    private final int[] degree;
    private int hashFuncIndex;
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

    public SketchGraph(int hashFuncIndex) {
        this.vCount = (short) Math.round(Math.sqrt(GlobalConfig.getECount() / GlobalConfig.getCompressionRate()));
        adjMatrix = new int[this.vCount][this.vCount];
        vertexHashTable = new ArrayList<>(this.vCount);
        for (int i = 0; i < this.vCount; i++) {
            vertexHashTable.add(new HashSet<Integer>());
        }
        degree = new int[this.vCount];
        this.hashFuncIndex = hashFuncIndex;
        edgeList = new ArrayList<>();
    }

    public int hashVertex(int vId){
        int hashCode = Hashing.sha256().hashInt(vId).hashCode();
        return Math.floorMod(hashCode * this.hashFunc[hashFuncIndex][0] + this.hashFunc[hashFuncIndex][1], this.vCount);
    }

    @Override
    public void readGraphFromFile() {
        try {
            InputStream inputStream = OriginGraph.class.getResourceAsStream(GlobalConfig.getInputGraphPath());
            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream));

            String line;
            while ((line = bufferedReader.readLine()) != null) {
                if (line.startsWith("#")) continue;
                String[] edgeValues = line.split("\t");
                int srcVid = Integer.parseInt(edgeValues[0]);
                int destVid = Integer.parseInt(edgeValues[1]);
                addEdge(srcVid, destVid);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        for(int i = 0; i < this.vCount; i++){
            for(int j = 0; j < this.vCount; j++){
                if(adjMatrix[i][j] == 0) continue;
                edgeList.add(new Edge(i, j, adjMatrix[i][j]));
            }
        }
    }

    @Override
    public void addEdge(int srcVId, int destVId) {
        int srcHash = hashVertex(srcVId);
        int destHash = hashVertex(destVId);

        vertexHashTable.get(srcHash).add(srcVId);
        vertexHashTable.get(destHash).add(destVId);
        adjMatrix[srcHash][destHash]++;
        degree[srcHash]++;
        if(srcHash != destHash) degree[destHash]++;
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

    @Override
    public int getECount() {
        return edgeList.size();
    }

    public int getDegree(int vid) {
        return degree[vid];
    }

    public int findWeight(int i, int j){
        return adjMatrix[i][j];
    }

    @Override
    public ArrayList<Edge> getEdgeList() {
        return edgeList;
    }

    @Override
    public void clear() {

    }
}
