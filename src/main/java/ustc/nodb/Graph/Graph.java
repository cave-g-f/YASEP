package ustc.nodb.Graph;

import ustc.nodb.core.Edge;

import java.util.ArrayList;

public interface Graph {
    public void readGraphFromFile();
    public void clear();
    public Edge readStep();
}
