package ustc.nodb.core;

public class Edge {
    private final int srcVId;
    private final int destVId;

    public Edge(int srcVId, int destVId) {
        this.srcVId = srcVId;
        this.destVId = destVId;
    }

    public int getSrcVId() {
        return srcVId;
    }

    public int getDestVId() {
        return destVId;
    }
}
