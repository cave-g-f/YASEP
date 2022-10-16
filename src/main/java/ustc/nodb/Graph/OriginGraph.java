package ustc.nodb.Graph;

import ustc.nodb.properties.*;
import ustc.nodb.core.*;
import java.io.*;

public class OriginGraph implements Graph
{
    private final int vCount;
    public BufferedReader bufferedReader;
    
    public OriginGraph() {
        this.vCount = GlobalConfig.getVCount();
    }
    
    @Override
    public Edge readStep() {
        try {
            final String line;
            if ((line = this.bufferedReader.readLine()) != null) {
                if (line.startsWith("#")) {
                    return this.readStep();
                }
                if (line.isEmpty()) {
                    return null;
                }
                final String[] edgeValues = line.split("\t");
                final int srcVid = Integer.parseInt(edgeValues[0]);
                final int destVid = Integer.parseInt(edgeValues[1]);
                if (srcVid == destVid) {
                    return this.readStep();
                }
                return new Edge(srcVid, destVid, 1);
            }
        }
        catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }
    
    @Override
    public void readGraphFromFile() {
        try {
            final File file = new File(GlobalConfig.getInputGraphPath());
            final FileReader fileReader = new FileReader(file);
            this.bufferedReader = new BufferedReader(fileReader);
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }
    
    @Override
    public void clear() {
        try {
            this.bufferedReader.close();
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }
}
