package ustc.nodb.core;

public class Node implements Comparable<Node>{
    private final int vId;
    private int inDegree;
    private int outDegree;

    public Node(int vId){
        this.vId = vId;
        inDegree = 0;
        outDegree = 0;
    }

    public void addInDegree(){
        this.inDegree++;
    }

    public void addOutDegree(){
        this.outDegree++;
    }

    public int getInDegree() {
        return inDegree;
    }

    public int getOutDegree() {
        return outDegree;
    }

    public int getId() {
        return vId;
    }

    @Override
    public int compareTo(Node node) {
        return Integer.compare(this.inDegree, node.getInDegree());
    }
}
