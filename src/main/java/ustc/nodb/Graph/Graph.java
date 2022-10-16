package ustc.nodb.Graph;

import ustc.nodb.core.*;

public interface Graph
{
    void readGraphFromFile();
    
    void clear();
    
    Edge readStep();
}
