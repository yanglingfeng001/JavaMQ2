package pku;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

/**
 * 从一个队列中读出一批消息，读出的消息要按照发送顺序来；
 * 这个接口需要是线程安全的，也即评测程序会并发调用该接口进行get；
 * 返回的Collection会被并发读，但不涉及写，因此只需要是线程读安全就可以了；
 *
 *  queueName 代表队列的名字
 *  offset    代表消息的在这个队列中的起始消息索引
 *  num       代表读取的消息的条数，如果消息足够，则返回num条，否则只返回已有的消息即可;没有消息了，则返回一个空的集合
 */
class Index {
    private List<IndexItem> index = new LinkedList<>();//利用一个链表来存储一个索引项的列表（或者说是数组）

    //一个用synchronized同步的add方法，将新的索引项添加到Index类总的索引数组中
    public synchronized void add(long offset, int startMsg, int msgNum) {
        index.add(new IndexItem(offset, startMsg, msgNum));//假设总是一段段按顺序添加的？？？？？？
    }
    public ArrayList<IndexItem> getBufIndexs(long offset, long num) {//获取指定大小num的索引数组的方法
        //一个IndexItem索引中有三项内容 偏移量是index存储在文件中的位置、信息开始时的索引位置或者是编号、消息大小
        //方法中的offset的意思是获取某队列中从offset处开始的num个消息的所有索引
        ArrayList<IndexItem> results = new ArrayList<>();
        boolean getFirst = false;
        for (int i = 0; i < index.size(); i++) {//操作是为了遍历一遍索引数组   还是  遍历索引数组的大小的次数？
            for (IndexItem indexItem : index) {
                if (indexItem.startMsgIndex <= offset
                        && indexItem.startMsgIndex + indexItem.msgNum > offset) {//offset位置在某一个indexItem中
                    results.add(indexItem);//将这个索引项加入到结果集合中
                    num -= (indexItem.msgNum - (offset - indexItem.startMsgIndex));//从此IndexItem中截取的大小为
                    // 此IndexItem的大小（即msgNum）-开头没被截取的一段（即offset - indexItem.startMsgIndex)）
                    //所以num要减去这个大小，表示还需要截取的数量需要多少
                    getFirst = true;//表示已经截取了一段了
                    continue;//寻找下一段了，就不需要以下两个if判断了，提升效率
                }
                if (getFirst && num > 0) {//已经截取了
                    results.add(indexItem);//添加到索引数组中
                    num -= indexItem.msgNum;//需要截取的消息数量减少
                    continue;
                }
                if (num <= 0)//这里有一个问题，如果num小于0的话，会不会添加的最后一个索引项的位置已经超出了offset+num的界限
                    return results;
            }
        }
        return null;
    }
}
