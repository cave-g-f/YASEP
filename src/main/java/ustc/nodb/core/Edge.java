package ustc.nodb.core;

public class Edge{
    private final int srcVId;
    private final int destVId;
    private int weight;
    private int clusterId;

    public Edge(int srcVId, int destVId, int weight) {
        this.srcVId = srcVId;
        this.destVId = destVId;
        this.weight = weight;
    }

    public Edge(int srcVId, int destVId, int weight, int clusterId){
        this.srcVId = srcVId;
        this.destVId = destVId;
        this.weight = weight;
        this.clusterId = clusterId;
    }

    public int getSrcVId() {
        return srcVId;
    }

    public int getDestVId() {
        return destVId;
    }

    public int getWeight() {
        return weight;
    }

    public int getClusterId(){
        return this.clusterId;
    }
}
