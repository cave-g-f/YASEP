package ustc.nodb.partitioner;

import ustc.nodb.Graph.Graph;
import ustc.nodb.cluster.Holl;
import ustc.nodb.core.Edge;
import ustc.nodb.main.TwoPSMain;
import ustc.nodb.properties.GlobalConfig;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;

public class TwoPS {

    private final HashMap<Integer, Integer> clusterToPartition;
    private final int[] partitionVolume;
    private final Holl holl;
    private final List<Integer> clusterList;
    private final int k;
    private final HashMap<Integer, HashSet<Integer>> replicatedTable;
    private final Graph graph;
    private final double maxVolume;

    public TwoPS(Holl holl, Graph graph) {
        this.clusterToPartition = new HashMap<>();
        this.partitionVolume = new int[GlobalConfig.partitionNum];
        this.k = GlobalConfig.partitionNum;
        this.holl = holl;
        this.clusterList = holl.getClusterList();
        this.replicatedTable = new HashMap<>();
        this.graph = graph;
        this.maxVolume = (double) GlobalConfig.eCount / (double) k * 1.0;
    }

    int getPartitionByScore(int src, int dest, int clusterSrc, int clusterDest) {
        int targetP = 0;
        double bestScore = 0.0;

        int degreeU = holl.getDegree(src);
        int degreeV = holl.getDegree(dest);
        int pU = clusterToPartition.get(clusterSrc);
        int pV = clusterToPartition.get(clusterDest);

        long sum, sum_of_volumes;
        int external_degrees_u, external_degrees_v;
        double max_score = 0;
        int max_p = 0;
        double bal, gv, gu, gv_c, gu_c;

        if (partitionVolume[pU] >= maxVolume || partitionVolume[pV] >= maxVolume) {
            if (degreeU > degreeV) {
                targetP = src % k;
            } else {
                targetP = dest % k;
            }

            if (partitionVolume[targetP] >= maxVolume) {
                int minLoad = Integer.MAX_VALUE;
                int minP = 0;
                for (int i = 0; i < k; i++) {
                    if (partitionVolume[i] < minLoad) {
                        minLoad = partitionVolume[i];
                        minP = i;
                    }
                }
                targetP = minP;
            }

            return targetP;
        } else {
            int[] ps = new int[2];
            ps[0] = clusterToPartition.get(clusterSrc);
            ps[1] = clusterToPartition.get(clusterDest);

            for (int p : ps) {
                if (partitionVolume[p] >= maxVolume) continue;

                gu = 0;
                gv = 0;
                gu_c = 0;
                gv_c = 0;
                sum = degreeU + degreeV;
                sum_of_volumes = holl.getVolume(clusterSrc) + holl.getVolume(clusterDest);

                if (replicatedTable.containsKey(src) && replicatedTable.get(src).contains(p)) {
                    gu = degreeU;
                    gu /= sum;
                    gu = 1 + (1 - gu);
                    if (clusterToPartition.get(clusterSrc).equals(p)) {
                        gu_c = holl.getVolume(clusterSrc);
                        gu_c /= sum_of_volumes;
                    }
                }

                if (replicatedTable.containsKey(dest) && replicatedTable.get(dest).contains(p)) {
                    gv = degreeV;
                    gv /= sum;
                    gv = 1 + (1 - gv);
                    if (clusterToPartition.get(clusterDest).equals(p)) {
                        gv_c = holl.getVolume(clusterDest);
                        gv_c /= sum_of_volumes;
                    }
                }

                double score_p = gu + gv + gu_c + gv_c;
                if (score_p < 0){
                    System.out.println("error");
                    System.out.println(sum);
                    System.out.println(sum_of_volumes);
                    System.exit(-1);
                }

                if (score_p >= max_score) {
                    max_score = score_p;
                    max_p = p;
                }

            }

        }

        return max_p;

    }


    public void clusterPartition() {
        int[] v = new int[k];
        for (Integer cluster : clusterList) {

            int targetP = 0;
            int minVolume = Integer.MAX_VALUE;
            for (int i = 0; i < k; i++) {
                if (v[i] < minVolume) {
                    targetP = i;
                    minVolume = v[i];
                }
            }

            if (!clusterToPartition.containsKey(cluster))
                clusterToPartition.put(cluster, 0);

            clusterToPartition.put(cluster, targetP);
            v[targetP] += holl.getVolume(cluster);

        }
    }

    public void prePartition() {
        graph.readGraphFromFile();
        Edge edge;
        while ((edge = graph.readStep()) != null) {
            int src = edge.getSrcVId();
            int dest = edge.getDestVId();
            int clusterSrc = holl.getClusterId(src);
            int clusterDest = holl.getClusterId(dest);

            if (clusterSrc == clusterDest || clusterToPartition.get(clusterSrc).equals(clusterToPartition.get(clusterDest))) {
                int targetP = clusterToPartition.get(clusterSrc);
                if (partitionVolume[targetP] >= (double) GlobalConfig.eCount / (double) k) {
                    targetP = getPartitionByScore(src, dest, clusterSrc, clusterDest);
                }

                if (!replicatedTable.containsKey(src)) replicatedTable.put(src, new HashSet<>());
                if (!replicatedTable.containsKey(dest)) replicatedTable.put(dest, new HashSet<>());

                partitionVolume[targetP]++;
                replicatedTable.get(src).add(targetP);
                replicatedTable.get(dest).add(targetP);
            }
        }
    }

    public void remainPartition() {
        graph.readGraphFromFile();
        Edge edge;
        while ((edge = graph.readStep()) != null) {
            int src = edge.getSrcVId();
            int dest = edge.getDestVId();
            int clusterSrc = holl.getClusterId(src);
            int clusterDest = holl.getClusterId(dest);

            if (clusterSrc == clusterDest || clusterToPartition.get(clusterSrc).equals(clusterToPartition.get(clusterDest)))
                continue;

            int targetP = getPartitionByScore(src, dest, clusterSrc, clusterDest);

            partitionVolume[targetP]++;
            if (!replicatedTable.containsKey(src)) replicatedTable.put(src, new HashSet<>());
            if (!replicatedTable.containsKey(dest)) replicatedTable.put(dest, new HashSet<>());
            replicatedTable.get(src).add(targetP);
            replicatedTable.get(dest).add(targetP);

        }
        graph.clear();
    }

    public double getReplicateFactor() {
        double sum = 0.0;
        for (Integer integer : replicatedTable.keySet()) {
            sum += replicatedTable.get(integer).size();
        }
        return sum / replicatedTable.size();
    }

    public double getLoadBalance() {
        double maxLoad = 0.0;
        for (int i = 0; i < k; i++) {
            if (maxLoad < partitionVolume[i]) {
                maxLoad = partitionVolume[i];
            }
        }
        return (double) k / GlobalConfig.getECount() * maxLoad;
    }


}
