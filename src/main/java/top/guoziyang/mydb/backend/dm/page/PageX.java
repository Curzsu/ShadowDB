package top.guoziyang.mydb.backend.dm.page;

import java.util.Arrays;

import top.guoziyang.mydb.backend.dm.pageCache.PageCache;
import top.guoziyang.mydb.backend.utils.Parser;

/**
 * PageX管理普通页
 * 核心流程：
 * PageX.java 是 MYDB 普通数据页的空间管理器，
 * 它在每个 8KB 页面的前 2 字节维护一个 FSO（Free Space Offset，空闲空间偏移量）指针，
 * 永远指向下一个可写入的位置；
 * 通过 insert() 方法实现 O(1) 时间复杂度的追加式数据写入（读 FSO → 写数据 → 更新 FSO），
 * 通过 getFreeSpace() 快速计算剩余空间，
 * 并提供 recoverInsert() 和 recoverUpdate() 两个方法支持数据库崩溃后的日志恢复,
 * 整个设计采用只增不减的追加模式，简单高效，
 * 但不支持删除和碎片整理，单条数据不能超过 8190 字节。
 * 
 * 普通页结构：
 * [FreeSpaceOffset] [Data]
 * FreeSpaceOffset: 2字节 空闲位置开始偏移
 * 
 * @author Lukesu
 * @date 2025-12-08
 * @version 1.0
 */

public class PageX {
    
    private static final short OF_FREE = 0; // 空闲空间开始偏移
    private static final short OF_DATA = 2; // 2字节偏移量(前2字节给FSO)
    public static final int MAX_FREE_SPACE = PageCache.PAGE_SIZE - OF_DATA; // 最大空闲空间

    // 初始化页面，设置FSO
    public static byte[] initRaw() {
        byte[] raw = new byte[PageCache.PAGE_SIZE]; // 8KB页面
        setFSO(raw, OF_DATA);   // 设置FSO，初始值为2, 表示前2字节给FSO
        return raw; // 返回初始化后的页面
    }

    /**
     * 设置FSO
     * @param raw 页面字节数组
     * @param ofData FSO的值
     */
    private static void setFSO(byte[] raw, short ofData) {
        // 将short转成2字节，写入raw的0-1位置
        System.arraycopy(Parser.short2Byte(ofData), 0, raw, OF_FREE, OF_DATA);
    }

    // 获取pg的FSO，pg指的是页面对象
    public static short getFSO(Page pg) {
        return getFSO(pg.getData());
    }

    // 获取raw的FSO，raw指的是页面字节数组
    private static short getFSO(byte[] raw) {
        // 从字节0-1读取2字节，转成short
        return Parser.parseShort(Arrays.copyOfRange(raw, 0, 2));
    }


    /**
     * 插入数据, 将raw插入pg中，返回插入位置
     * @param pg 页面
     * @param raw 数据
     * @return 插入位置
     */
    public static short insert(Page pg, byte[] raw) {
        pg.setDirty(true);
        short offset = getFSO(pg.getData());    // 获取FSO
        System.arraycopy(raw, 0, pg.getData(), offset, raw.length); // 将raw插入pg中
        setFSO(pg.getData(), (short)(offset + raw.length)); // 更新FSO
        return offset;  // 返回插入位置
    }

    // 获取页面的空闲空间大小
    public static int getFreeSpace(Page pg) {
        return PageCache.PAGE_SIZE - (int)getFSO(pg.getData());
    }

    // 将raw插入pg中的offset位置，并将pg的offset设置为较大的offset
    public static void recoverInsert(Page pg, byte[] raw, short offset) {
        pg.setDirty(true);  // 设置页面为脏页面
        System.arraycopy(raw, 0, pg.getData(), offset, raw.length);  // 将raw复制到pg的offset位置

        short rawFSO = getFSO(pg.getData());  // 获取pg的FSO
        if(rawFSO < offset + raw.length) {  // 如果raw的FSO小于offset+raw.length
            setFSO(pg.getData(), (short)(offset+raw.length));  // 更新FSO
        }
    }

    // 将raw插入pg中的offset位置，不更新update
    public static void recoverUpdate(Page pg, byte[] raw, short offset) {
        pg.setDirty(true);
        System.arraycopy(raw, 0, pg.getData(), offset, raw.length);
    }
}
