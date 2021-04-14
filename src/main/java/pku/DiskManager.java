package pku;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.ConcurrentHashMap;

public class DiskManager {
    private static RandomAccessFile randomAccessFile ;//读写流 类似于datainput 和dataoutput
    private static ConcurrentHashMap<Thread, FileChannel> readFileChannels = new ConcurrentHashMap<>();
    static {
        try {
            randomAccessFile = new RandomAccessFile("data/queue_data", "rw");//读写模式创建一个文件流
        } catch (FileNotFoundException e) {
            e.printStackTrace();
            System.exit(-1);
        }
    }

    public static synchronized void writeToFile(ByteBuffer buffer, long position) {//写入
        FileChannel channel = randomAccessFile.getChannel();
        try {
            channel.write(buffer, position);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void readAbBuf(ByteBuffer buffer, long position) {//读取
        if (!readFileChannels.keySet().contains(Thread.currentThread())) {//不存在当前线程操作的文件通道
            readFileChannels.put(Thread.currentThread(), randomAccessFile.getChannel());//创建一个文件通道
        }
        FileChannel channel = readFileChannels.get(Thread.currentThread());//获取
        try {
            channel.read(buffer, position);//利用通道的方法读取文件中从positon的内容到buffer
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
