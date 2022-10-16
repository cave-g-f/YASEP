package ustc.nodb.IO;

import ustc.nodb.properties.*;
import java.io.*;
import java.util.*;
import java.nio.*;
import java.nio.channels.*;
import java.util.concurrent.*;
import java.nio.file.*;

public class BatchFileIO
{
    private final HashMap<Integer, String> batchFileName;
    private final HashMap<Integer, FileOutputStream> fileOutPutHandler;
    private final HashMap<Integer, FileInputStream> fileInputHandler;
    private final AsynFileIO asynFileIO;
    
    public BatchFileIO() {
        this.fileOutPutHandler = new HashMap<Integer, FileOutputStream>();
        this.fileInputHandler = new HashMap<Integer, FileInputStream>();
        this.batchFileName = new HashMap<Integer, String>();
        this.asynFileIO = new AsynFileIO();
    }
    
    public void outputFilesAsyn(final HashMap<Integer, ArrayList<Integer>> buffer) {
        this.asynFileIO.outputFiles(buffer);
    }
    
    public boolean isDone() {
        return this.asynFileIO.isDone();
    }
    
    public String getFileName(final int batchId) {
        if (!this.batchFileName.containsKey(batchId)) {
            this.batchFileName.put(batchId, GlobalConfig.batchEdgePath + batchId + ".bin");
        }
        return this.batchFileName.get(batchId);
    }
    
    public void outputFiles(final HashMap<Integer, ArrayList<Integer>> buffer) {
        for (final Map.Entry<Integer, ArrayList<Integer>> entry : buffer.entrySet()) {
            final int batchID = entry.getKey();
            final String filename = this.getFileName(batchID);
            if (!this.fileOutPutHandler.containsKey(batchID)) {
                final File file = new File(filename);
                try {
                    final FileOutputStream fileOutputStream = new FileOutputStream(file);
                    this.fileOutPutHandler.put(batchID, fileOutputStream);
                }
                catch (FileNotFoundException e) {
                    e.printStackTrace();
                }
            }
            final FileChannel out = this.fileOutPutHandler.get(batchID).getChannel();
            final ByteBuffer byteBuffer = ByteBuffer.allocateDirect(4 * entry.getValue().size());
            final IntBuffer intBuffer = byteBuffer.asIntBuffer();
            for (final Integer v : entry.getValue()) {
                intBuffer.put(v);
            }
            try {
                out.write(byteBuffer);
            }
            catch (IOException e2) {
                e2.printStackTrace();
            }
            byteBuffer.clear();
            intBuffer.clear();
        }
        buffer.clear();
        System.gc();
    }
    
    public void checkBatchFile() {
        long totalLength = 0L;
        for (final Map.Entry<Integer, String> entry : this.batchFileName.entrySet()) {
            final int batchID = entry.getKey();
            final File file = new File(GlobalConfig.batchEdgePath + batchID + ".bin");
            totalLength += file.length();
        }
        if (totalLength / 4L / 3L == GlobalConfig.eCount) {
            System.out.println("file format correct!");
        }
        else {
            System.out.println("file format wrong!, totalLength: " + totalLength);
        }
    }
    
    public void closeOutput() {
        for (final Map.Entry<Integer, FileOutputStream> entry : this.fileOutPutHandler.entrySet()) {
            try {
                entry.getValue().close();
            }
            catch (IOException e) {
                e.printStackTrace();
            }
        }
        this.fileOutPutHandler.clear();
        System.gc();
    }
    
    public void closeOutputAsyn() {
        this.asynFileIO.closeOutput();
    }
    
    public void closeInput() {
        for (final Map.Entry<Integer, FileInputStream> entry : this.fileInputHandler.entrySet()) {
            try {
                entry.getValue().close();
            }
            catch (IOException e) {
                e.printStackTrace();
            }
        }
        this.fileInputHandler.clear();
        System.gc();
    }
    
    public FileInputStream getFileInputHandler(final int batchId) {
        final String filename = this.batchFileName.get(batchId);
        if (!this.fileInputHandler.containsKey(batchId)) {
            try {
                final File file = new File(filename);
                final FileInputStream fileInputStream = new FileInputStream(file);
                this.fileInputHandler.put(batchId, fileInputStream);
            }
            catch (FileNotFoundException e) {
                e.printStackTrace();
            }
        }
        return this.fileInputHandler.get(batchId);
    }
    
    class AsynFileIO
    {
        private final HashMap<Integer, AsynchronousFileChannel> fileOutPutHandlerAsyn;
        private final HashMap<Integer, Long> filePos;
        private final ArrayList<Future<Integer>> taskList;
        
        AsynFileIO() {
            this.fileOutPutHandlerAsyn = new HashMap<Integer, AsynchronousFileChannel>();
            this.filePos = new HashMap<Integer, Long>();
            this.taskList = new ArrayList<Future<Integer>>();
        }
        
        public void outputFiles(final HashMap<Integer, ArrayList<Integer>> buffer) {
            for (final Map.Entry<Integer, ArrayList<Integer>> entry : buffer.entrySet()) {
                final int batchID = entry.getKey();
                final String filename = BatchFileIO.this.getFileName(batchID);
                if (!this.filePos.containsKey(batchID)) {
                    this.filePos.put(batchID, 0L);
                }
                try {
                    if (!this.fileOutPutHandlerAsyn.containsKey(batchID)) {
                        final File file = new File(filename);
                        if (file.exists()) {
                            file.delete();
                        }
                        file.createNewFile();
                        final Path path = Paths.get(BatchFileIO.this.batchFileName.get(batchID), new String[0]);
                        final AsynchronousFileChannel fileChannel = AsynchronousFileChannel.open(path, StandardOpenOption.WRITE);
                        this.fileOutPutHandlerAsyn.put(batchID, fileChannel);
                    }
                }
                catch (IOException ex) {
                    ex.printStackTrace();
                }
                final ByteBuffer byteBuffer = ByteBuffer.allocate(4 * entry.getValue().size());
                final IntBuffer intBuffer = byteBuffer.asIntBuffer();
                for (final Integer v : entry.getValue()) {
                    intBuffer.put(v);
                }
                final AsynchronousFileChannel channel = this.fileOutPutHandlerAsyn.get(batchID);
                final Future<Integer> future = channel.write(byteBuffer, this.filePos.get(batchID));
                this.taskList.add(future);
                this.filePos.put(batchID, this.filePos.get(batchID) + 4 * entry.getValue().size());
            }
        }
        
        public boolean isDone() {
            for (final Future<Integer> task : this.taskList) {
                if (!task.isDone()) {
                    return false;
                }
            }
            this.taskList.clear();
            return true;
        }
        
        public void closeOutput() {
            for (final Map.Entry<Integer, AsynchronousFileChannel> entry : this.fileOutPutHandlerAsyn.entrySet()) {
                try {
                    entry.getValue().close();
                }
                catch (IOException e) {
                    e.printStackTrace();
                }
            }
            this.fileOutPutHandlerAsyn.clear();
            System.gc();
        }
    }
}
