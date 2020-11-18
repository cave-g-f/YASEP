package ustc.nodb.core;

import ustc.nodb.properties.GlobalConfig;

import java.io.*;
import java.util.ArrayList;

public class Graph {

    private final ArrayList<Edge> edgeList;
    private final int vCount;
    private final int eCount;

    public Graph() {
        this.edgeList = new ArrayList<>();
        this.vCount = GlobalConfig.getVCount();
        this.eCount = GlobalConfig.getECount();
    }

    public void readGraphFromFile() {
        try {
            InputStream inputStream = Graph.class.getResourceAsStream(GlobalConfig.getInputGraphPath());
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
    }

    private void addEdge(int srcVId, int destVId) {
        Edge edge = new Edge(srcVId, destVId, 0);
        edgeList.add(edge);
    }

    public ArrayList<Edge> getEdgeList() {
        return edgeList;
    }

    public int getVCount() {
        return vCount;
    }

    public int getECount() {
        return eCount;
    }
}
