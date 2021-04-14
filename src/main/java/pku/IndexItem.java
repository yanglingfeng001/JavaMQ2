package pku;

class IndexItem {
    public long offset;//文件中的存储位置
    public int startMsgIndex;//开始消息在某个队列中的编号
    public int msgNum;//消息数量

    public IndexItem(long offset, int startMsgIndex, int msgNum) {
        this.offset = offset;
        this.startMsgIndex = startMsgIndex;
        this.msgNum = msgNum;
    }
}
