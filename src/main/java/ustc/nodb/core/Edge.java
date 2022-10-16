package ustc.nodb.core;

public class Edge
{
    private final int srcVId;
    private final int destVId;
    private int weight;
    private int clusterId;
    
    public Edge(final int srcVId, final int destVId, final int weight) {
        this.srcVId = srcVId;
        this.destVId = destVId;
        this.weight = weight;
    }
    
    public Edge(final int srcVId, final int destVId, final int weight, final int clusterId) {
        this.srcVId = srcVId;
        this.destVId = destVId;
        this.weight = weight;
        this.clusterId = clusterId;
    }
    
    public int getSrcVId() {
        return this.srcVId;
    }
    
    public int getDestVId() {
        return this.destVId;
    }
    
    public int getWeight() {
        return this.weight;
    }
    
    public int getClusterId() {
        return this.clusterId;
    }
}
