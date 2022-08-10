package ustc.nodb.partitioner;

import java.io.IOException;

public interface PartitionStrategy {
    public void performStep();
    public void clear();
    public double getReplicateFactor();
    public double getLoadBalance();
}
