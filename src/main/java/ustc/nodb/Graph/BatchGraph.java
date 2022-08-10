package ustc.nodb.Graph;

import ustc.nodb.IO.BatchFileIO;
import ustc.nodb.core.Edge;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.IntBuffer;

public class BatchGraph implements Graph{

    private final FileInputStream fileInputStream;
    private final BatchFileIO batchFileIO;
    private int batchId;

    public BatchGraph(BatchFileIO batchFileIO, int batchId) {
        this.batchFileIO = batchFileIO;
        this.batchId = batchId;
        fileInputStream = batchFileIO.getFileInputHandler(batchId);
    }


    @Override
    public void readGraphFromFile() {
        try {
            fileInputStream.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void clear() {
    }

    @Override
    public Edge readStep() {
        byte[] bytes = new byte[3 * Integer.BYTES];
        int len = 0;
        try{
            if((len = this.fileInputStream.read(bytes)) != -1){
                IntBuffer intBuffer = ByteBuffer.wrap(bytes).order(ByteOrder.BIG_ENDIAN).asIntBuffer();
                int src = intBuffer.get();
                int dest = intBuffer.get();
                int clusterId = intBuffer.get();
//                System.out.println(src + " " + dest + " " + clusterId);
                return new Edge(src, dest, 1, clusterId);
            }
        }catch (Exception ex){
            ex.printStackTrace();
        }
        return null;
    }
}
