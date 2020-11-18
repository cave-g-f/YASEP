package ustc.nodb.core;

public class Edge implements Comparable<Edge>{
    private final int srcVId;
    private final int destVId;
    private final int weight;

    public Edge(int srcVId, int destVId, int weight) {
        this.srcVId = srcVId;
        this.destVId = destVId;
        this.weight = weight;
    }

    public int getSrcVId() {
        return srcVId;
    }

    public int getDestVId() {
        return destVId;
    }

    @Override
    public int compareTo(Edge edge) {
        if(this.weight < edge.weight) return -1;
        else if(this.weight > edge.weight) return 1;
        else return 0;
    }
}
