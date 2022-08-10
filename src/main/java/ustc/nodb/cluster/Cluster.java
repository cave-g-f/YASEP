package ustc.nodb.cluster;

import java.util.HashSet;

public class Cluster  {

    int volume;
    int id;
    HashSet<Integer> repVertex;

    public Cluster(int id){
        this.id = id;
        this.volume = 0;
        this.repVertex = new HashSet<>();
    }

    public int getId(){
        return id;
    }

    public int getVolume(){
        return volume;
    }

    public HashSet<Integer> getRepVertex(){
        return repVertex;
    }

    @Override
    public int hashCode(){
        return Integer.hashCode(id);
    }

    @Override
    public boolean equals(Object obj){
        if(obj == null) return false;
        if(obj.getClass() != this.getClass()) return false;
        Cluster c = (Cluster) obj;
        return c.id == this.id;
    }
}

