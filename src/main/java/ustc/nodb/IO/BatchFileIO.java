package ustc.nodb.IO;

import ustc.nodb.properties.GlobalConfig;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Future;

public class BatchFileIO {
    private final HashMap<Integer, String> batchFileName;
    private final HashMap<Integer, FileOutputStream> fileOutPutHandler;
    private final HashMap<Integer, FileInputStream> fileInputHandler;
    private final AsynFileIO asynFileIO;


    class AsynFileIO{
        private final HashMap<Integer, AsynchronousFileChannel> fileOutPutHandlerAsyn;
        private final HashMap<Integer, Long> filePos;
        private final ArrayList<Future<Integer>> taskList;

        AsynFileIO() {
            this.fileOutPutHandlerAsyn = new HashMap<>();
            this.filePos = new HashMap<>();
            this.taskList = new ArrayList<>();
        }

        public void outputFiles(HashMap<Integer, ArrayList<Integer>> buffer){
            for (Map.Entry<Integer, ArrayList<Integer>> entry : buffer.entrySet()) {
                int batchID = entry.getKey();
                String filename = getFileName(batchID);

                if(!filePos.containsKey(batchID))
                {
                    filePos.put(batchID, 0L);
                }

                try {
                    if (!fileOutPutHandlerAsyn.containsKey(batchID)) {
                        File file = new File(filename);
                        if(file.exists()) file.delete();
                        file.createNewFile();
                        Path path = Paths.get(batchFileName.get(batchID));
                        AsynchronousFileChannel fileChannel = AsynchronousFileChannel.open(path, StandardOpenOption.WRITE);
                        this.fileOutPutHandlerAsyn.put(batchID, fileChannel);
                    }
                }catch (IOException ex){
                    ex.printStackTrace();
                }

                ByteBuffer byteBuffer = ByteBuffer.allocate(Integer.BYTES * entry.getValue().size());
                IntBuffer intBuffer = byteBuffer.asIntBuffer();
                for (Integer v : entry.getValue()) {
                    intBuffer.put(v);
                }

                // start asynchronous write
                AsynchronousFileChannel channel = this.fileOutPutHandlerAsyn.get(batchID);
                Future<Integer> future = channel.write(byteBuffer, filePos.get(batchID));
                taskList.add(future);
                filePos.put(batchID, filePos.get(batchID) + Integer.BYTES * entry.getValue().size());
            }
        }

        public boolean isDone()
        {
            for(Future<Integer> task : taskList){
                if(!task.isDone()) return false;
            }
            taskList.clear();
            return true;
        }

        public void closeOutput() {
            for (Map.Entry<Integer, AsynchronousFileChannel> entry : fileOutPutHandlerAsyn.entrySet()) {
                try {
                    entry.getValue().close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            fileOutPutHandlerAsyn.clear();
            System.gc();
        }
    }

    public BatchFileIO() {
        this.fileOutPutHandler = new HashMap<>();
        this.fileInputHandler = new HashMap<>();
        this.batchFileName = new HashMap<>();
        this.asynFileIO = new AsynFileIO();
    }

    public void outputFilesAsyn(HashMap<Integer, ArrayList<Integer>> buffer){
        this.asynFileIO.outputFiles(buffer);
    }

    public boolean isDone(){
        return asynFileIO.isDone();
    }

    public String getFileName(int batchId) {
        if (!this.batchFileName.containsKey(batchId)) {
            this.batchFileName.put(batchId, GlobalConfig.batchEdgePath + batchId + ".bin");
        }

        return this.batchFileName.get(batchId);
    }

    public void outputFiles(HashMap<Integer, ArrayList<Integer>> buffer) {
        for (Map.Entry<Integer, ArrayList<Integer>> entry : buffer.entrySet()) {
            int batchID = entry.getKey();
            String filename = getFileName(batchID);
            if (!fileOutPutHandler.containsKey(batchID)) {
                File file = new File(filename);
                try {
                    FileOutputStream fileOutputStream = new FileOutputStream(file);
                    this.fileOutPutHandler.put(batchID, fileOutputStream);
                } catch (FileNotFoundException e) {
                    e.printStackTrace();
                }
            }

            FileChannel out = fileOutPutHandler.get(batchID).getChannel();
            ByteBuffer byteBuffer = ByteBuffer.allocateDirect(Integer.BYTES * entry.getValue().size());
            IntBuffer intBuffer = byteBuffer.asIntBuffer();
            for (Integer v : entry.getValue()) {
                intBuffer.put(v);
            }
            try {
                out.write(byteBuffer);
            } catch (IOException e) {
                e.printStackTrace();
            }
            byteBuffer.clear();
            intBuffer.clear();
        }
        buffer.clear();
        System.gc();
    }

    public void checkBatchFile() {
        long totalLength = 0;
        for (Map.Entry<Integer, String> entry : batchFileName.entrySet()) {
            int batchID = entry.getKey();
            File file = new File(GlobalConfig.batchEdgePath + batchID + ".bin");
            totalLength += file.length();
        }

        if (totalLength / Integer.BYTES / 3 == GlobalConfig.eCount) {
            System.out.println("file format correct!");
        } else {
            System.out.println("file format wrong!, totalLength: " + totalLength);
        }
    }

    public void closeOutput() {
        for (Map.Entry<Integer, FileOutputStream> entry : fileOutPutHandler.entrySet()) {
            try {
                entry.getValue().close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        fileOutPutHandler.clear();
        System.gc();
    }

    public void closeOutputAsyn() {
        this.asynFileIO.closeOutput();
    }

    public void closeInput() {
        for (Map.Entry<Integer, FileInputStream> entry : fileInputHandler.entrySet()) {
            try {
                entry.getValue().close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        fileInputHandler.clear();
        System.gc();
    }

    public FileInputStream getFileInputHandler(int batchId) {
        String filename = this.batchFileName.get(batchId);
        if (!this.fileInputHandler.containsKey(batchId)) {
            try {
                File file = new File(filename);
                FileInputStream fileInputStream = new FileInputStream(file);
                fileInputHandler.put(batchId, fileInputStream);
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            }
        }
        return this.fileInputHandler.get(batchId);
    }

}
