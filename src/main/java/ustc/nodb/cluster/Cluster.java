package ustc.nodb.cluster;

import java.util.*;

public class Cluster
{
    public int volume;
    public int id;
    public HashSet<Integer> repVertex;
    public int vertexCnt;
    
    public Cluster(final int id) {
        this.id = id;
        this.volume = 0;
        this.repVertex = new HashSet<Integer>();
    }
    
    @Override
    public int hashCode() {
        return Integer.hashCode(this.id);
    }
    
    @Override
    public boolean equals(final Object obj) {
        if (obj == null) {
            return false;
        }
        if (obj.getClass() != this.getClass()) {
            return false;
        }
        final Cluster c = (Cluster)obj;
        return c.id == this.id;
    }
}
