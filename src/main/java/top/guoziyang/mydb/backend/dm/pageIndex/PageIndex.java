package top.guoziyang.mydb.backend.dm.pageIndex;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import top.guoziyang.mydb.backend.dm.pageCache.PageCache;


/**
 * PageIndex 负责管理页面的空闲空间，
 * 通过将页面划分为多个区间，并使用链表实现空闲空间的快速分配和回收，从而支持高效的页面空间管理。
 * 它的设计思想类似于内存分配器中的分级空闲链表（segregated free list）。
 * 
 * 核心流程：
 * PageIndex.java 是 MYDB 的"页面空间查询器"，
 * 它将 8KB 页面按空闲空间大小分成 40 个区间（每个区间约 200 字节），
 * 用数组 List<PageInfo>[] 管理这些页面，
 * 通过 add() 方法快速将页面按空闲空间分类存储，
 * 通过 select() 方法采用"就近向上搜索"策略（从所需空间所在区间开始，逐步向上查找）
 * 快速找到最合适的页面
 * 本质上是一个分桶式的空间索引，用最简单的数据结构实现了 O(1)~O(40) 的高效页面选择，
 * 大大提升了插入数据的性能。
 * 
 * @author Lukesu
 * @date 2025-12-08
 * @version 1.0
 */

public class PageIndex {
    // 将一页划成40个区间
    private static final int INTERVALS_NO = 40; // 区间数量

    /** 
     * THRESHOLD：每个区间的阈值，即每个区间的空闲空间大小, 单位字节
     * 假设页面大小是 8KB，那么 THRESHOLD = 8192 / 40 ≈ 204 字节
     * 第 0 区间存储空闲空间为 0-204 字节的页面
     * 第 1 区间存储 205-408 字节的页面
     * 依此类推
    */
    private static final int THRESHOLD = PageCache.PAGE_SIZE / INTERVALS_NO; 


    private Lock lock;  // 用于并发控制，保证线程安全
    private List<PageInfo>[] lists; // 41个列表数组（0-40），即空闲空间管理数组，每个元素是一个链表，存储PageInfo对象


    /**
     * PageIndex 构造函数
     * 初始化锁和空闲空间管理数组
     */
    @SuppressWarnings("unchecked") // 告诉编译器，我知道我在做 unchecked 操作，不要给我报 warning
    public PageIndex() {
        lock = new ReentrantLock();
        lists = new List[INTERVALS_NO+1];
        for (int i = 0; i < INTERVALS_NO+1; i ++) {
            lists[i] = new ArrayList<>();
        }
    }


    /**
     * 根据空闲空间大小计算区间编号,将页面信息添加到对应区间列表,并用锁保证线程安全
     * @param pgno 页面编号
     * @param freeSpace 页面空闲空间
     */
    public void add(int pgno, int freeSpace) {
        lock.lock();
        try {
            int number = freeSpace / THRESHOLD; // 计算所属区间
            lists[number].add(new PageInfo(pgno, freeSpace));
        } finally {
            lock.unlock();
        }
    }

    /**
     * 查找合适的页面，根据空间大小选择页面,并用锁保证线程安全
     * 如果找不到合适的页面，则通过 PageCache.newPage() 创建新页并加入索引
     * @param spaceSize 空间大小
     * @return 页面信息
     */
    public PageInfo select(int spaceSize) {
        lock.lock();
        try {
            int number = spaceSize / THRESHOLD;
            if(number < INTERVALS_NO) number ++; // 向上取区间
            while(number <= INTERVALS_NO) {
                if(lists[number].size() == 0) {
                    number ++;
                    continue;
                }
                return lists[number].remove(0); // 返回并移除链表头部元素
            }
            return null;    // 没有找到合适的页面
        } finally {
            lock.unlock();
        }
    }

}
