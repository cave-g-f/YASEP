package ustc.nodb.game;

import org.junit.Test;
import ustc.nodb.Graph.Graph;
import ustc.nodb.Graph.OriginGraph;
import ustc.nodb.cluster.StreamCluster;
import ustc.nodb.partitioner.TcmSp;

import java.util.ArrayList;
import java.util.HashSet;

import static org.junit.Assert.*;

public class ClusterPackGameTest {

    Graph graph;

    public ClusterPackGameTest(){
        graph = new OriginGraph();
    }

    @Test
    public void Test(){
        graph.readGraphFromFile();

        StreamCluster streamCluster = new StreamCluster(graph);
        streamCluster.startSteamCluster();

        System.out.println(streamCluster.getClusterList().size());

        ClusterPackGame clusterPackGame = new ClusterPackGame(streamCluster);
        clusterPackGame.startGame();

        ArrayList<HashSet<Integer>> partition = clusterPackGame.getInvertedPartitionIndex();
        System.out.println("cut edge: " + clusterPackGame.getCutEdge());

        for(HashSet<Integer> clusters : partition){
            System.out.println("num : " + clusters.size());
        }

        TcmSp tcmSp = new TcmSp(graph, streamCluster, clusterPackGame);
        tcmSp.performStep();

        System.out.println(tcmSp.getLoadBalance());
        System.out.println(tcmSp.getReplicateFactor());
    }

}