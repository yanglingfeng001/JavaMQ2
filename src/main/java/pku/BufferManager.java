package pku;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

public class BufferManager {//封装缓冲区的一些数据和对缓冲区的一些操作
    private static AtomicLong bufIndex = new AtomicLong();//自增原子long数据
    private static final int buf_size = 8 << 10;//8*2的10次方骚操作表示 8192 //但是这里队列缓冲区大小是8KB
    private volatile boolean isFirstGet = true;//为什么这里要使用volatile来修饰？？？？？
    //被volatile修饰的变量能够保证每个线程能够获取该变量的最新值，从而避免出现数据脏读的现象。
    private ByteBuffer buffer = ByteBuffer.allocateDirect(buf_size);//给buffer缓冲区分配buf_size大小的内存 8192
    //由于频繁的本地IO所以使用ByteBuffer.allocateDirect()方法
    private int currentMsgNum = 0;
    private int bufferMagNum = 0;
    private Index index = new Index();

    public void put(byte[] message) {//将byte数组压入缓冲区，这里的message.length是以字节为单位的
        if (message.length + 2 > buffer.remaining()) {//如果超出缓冲区的大小先写入本地磁盘 //但是为什么要加2，
            // 是因为需要写入message.length，是short，short的大小是16位，刚好为2个Byte
            // 应该是和之前的消息隔1位外加写入message.length
            writeToDisk();//写入本地磁盘方法
            buffer.clear();//清除缓冲区
        }
        buffer.putShort((short) message.length);
        buffer.put(message);//写入信息
        currentMsgNum++;//
        bufferMagNum++;//一会儿再看
    }


    public synchronized Collection<byte[]> get(long offset, long num) {//读取消息到buffer
        // 然后从offset开始读取num长度的消息数量（非大小）
        if (isFirstGet) {//是第一次读先把当前缓冲区的消息写入磁盘
            writeToDisk();
            isFirstGet = false;//设置不是第一次读的标志
        }
        List<byte[]> result = new ArrayList<>();//用于接收消息，返回结果
        ArrayList<IndexItem> bufIndexs = index.getBufIndexs(offset, num);//获取offset，即在某个消息队列中的起始索引
        // 为开始，大小（或者说是数量）为num的消息的所拥有的
        // 所有索引项组成的数组

        //如果索引数组为空或者索引数组大小为0，换句话说就是没有相应的消息存在
        if (bufIndexs == null || bufIndexs.size() == 0) return DefaultQueueStoreImpl.EMPTY;//返回一个空的集合
        //但是为什么必须返回DefaultQueueStoreImpl中的Empty，不能返回new ArrayList<IndexItem>()？
        //因为可能会爆内存 static修饰只初始化一次，强制返回同一个实例

        int leftNum = (int)num;//还需要读取的消息数量
        //num强转为int，是否可能会丢失？
        // 不会丢失，这里可能取的num只有那么大，要不然就会冲出缓冲区的大小？？

        boolean isFirst = true;//此标识有什么作用？？ 标识是否是读的第一个索引块


        for (IndexItem bufIndex : bufIndexs) {//遍历获取的索引数组
            buffer.clear();//每次都要先清除缓存
            DiskManager.readAbBuf(buffer, bufIndex.offset);//从offset位置开始 读取一个8K的数据块到buffer缓存中去
            buffer.position(0);//将buffer的读取指针（可以这样理解？）放在buffer首
            int startIndex = 0;//首位置？
            if (isFirst) {
                startIndex = (int) offset - bufIndex.startMsgIndex;//在某个索引中需要
                // 跳过的位置，当然如果不是第一个读的索引自然不用跳过，所以使用isFirst标识来判断
                isFirst = false;
            }
            int canReadNum = bufIndex.msgNum - startIndex;//能够真实读取的消息大小
            int realNum = leftNum < canReadNum ? leftNum : canReadNum;//某个索引项中真实读取的消息大小
            skipNMsgInBuf(startIndex);//跳转到需要读取消息的某个索引项的真实位置，如果不是首个索引则不用跳转，即跳转0
            for (int i = 0; i < realNum; i++) {
                byte[] nextMsgInBuf = getNextMsgInBuf();
                result.add(nextMsgInBuf);
            }
            leftNum -= realNum;
        }
        return result;
    }

    public void skipNMsgInBuf(int n) {//从缓冲区中跳过n个消息，将缓冲区首个读取的位置指向真实开始需要读取的位置
        for (int i = 0; i < n; i++) {
            short msgLen = buffer.getShort();//获取消息大小
            int oldPos = buffer.position();
            buffer.position(oldPos + msgLen);//最终作用是放置指针
        }
    }

    public byte[] getNextMsgInBuf() {//从缓冲区中获取下个消息
        short msgLen = buffer.getShort();//缓冲区中消息的形势是消息大小+消息内容，而消息最多8Kb，
        // 所以消息的最大小小可以用16位来表示，即一个short的大小
        byte[] message = new byte[msgLen];//获取消息的byte数组
        buffer.get(message);//将从缓冲区中获取的消息的byte数组赋值给message以供返回
        return message;
    }
    private void writeToDisk() {
        buffer.position(0);//缓冲区指到开头
        long curBufIndex = bufIndex.getAndIncrement();//当前的缓冲区索引位置为获取后再自增1

        //这里没有能看懂
        index.add(curBufIndex * buf_size, currentMsgNum-bufferMagNum, bufferMagNum);
        //offset代表索引指向的消息块在文件中存储的位置，startMsg代表此消息r在当前队列中的消息编号，bufferMagNum代表消息大小

        DiskManager.writeToFile(buffer, curBufIndex * buf_size);//每一块确定一个大小
        buffer.clear();
        bufferMagNum = 0;//写入磁盘结束后缓冲区中消息的数量重新设置为0
    }

}
