package ustc.nodb.Graph;

import ustc.nodb.IO.*;
import java.io.*;
import ustc.nodb.core.*;
import java.nio.*;

public class BatchGraph implements Graph
{
    private final FileInputStream fileInputStream;
    private final BatchFileIO batchFileIO;
    private int batchId;
    
    public BatchGraph(final BatchFileIO batchFileIO, final int batchId) {
        this.batchFileIO = batchFileIO;
        this.batchId = batchId;
        this.fileInputStream = batchFileIO.getFileInputHandler(batchId);
    }
    
    @Override
    public void readGraphFromFile() {
        try {
            this.fileInputStream.close();
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }
    
    @Override
    public void clear() {
    }
    
    @Override
    public Edge readStep() {
        final byte[] bytes = new byte[12];
        int len = 0;
        try {
            if ((len = this.fileInputStream.read(bytes)) != -1) {
                final IntBuffer intBuffer = ByteBuffer.wrap(bytes).order(ByteOrder.BIG_ENDIAN).asIntBuffer();
                final int src = intBuffer.get();
                final int dest = intBuffer.get();
                final int clusterId = intBuffer.get();
                return new Edge(src, dest, 1, clusterId);
            }
        }
        catch (Exception ex) {
            ex.printStackTrace();
        }
        return null;
    }
}
