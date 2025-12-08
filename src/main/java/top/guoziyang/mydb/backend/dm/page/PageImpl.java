package top.guoziyang.mydb.backend.dm.page;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import top.guoziyang.mydb.backend.dm.pageCache.PageCache;


/**
 * PageImpl 是 Page 接口的实现类
 * 一个 PageImpl 对象对应一个页面，包含页面数据、页面编号、脏页标记、页面锁和 PageCache 引用
 * 核心流程：
 * PageImpl.java 是页面对象的内存表示，
 * 它持有一个 8KB 的字节数组存储实际数据、
 * 一个页号标识其在文件中的位置、一个脏标记标志是否被修改、
 * 一个锁对象保证并发安全，
 * 以及对 PageCache 的引用用于快速释放自身——本质上它就是磁盘页面在内存中的"代理"，
 * 负责被上层模块（如 DataItem 和 PageX）操作数据，
 * 并在缓存驱逐时通过 PageCache 实现与磁盘的同步。
 * 
 * @author Lukesu
 * @date 2025-12-08
 * @version 1.0
 */

public class PageImpl implements Page {
    private int pageNumber;  // 该页面的页号，从 1 开始
    private byte[] data;  // 页面数据
    private boolean dirty;  // 脏页标记（是否被修改过），在缓存驱逐的时候，脏页面需要被写回磁盘
    private Lock lock;  // 页面锁，用于并发控制
    
    private PageCache pc;   // 用来方便在拿到 Page 的引用时可以快速对这个页面的缓存进行释放操作

    public PageImpl(int pageNumber, byte[] data, PageCache pc) {
        this.pageNumber = pageNumber;
        this.data = data;
        this.pc = pc;
        lock = new ReentrantLock(); // 多个线程可能同时操作同一页面，必须加锁保护
    }

    public void lock() {
        lock.lock();
    }

    public void unlock() {
        lock.unlock();
    }

    /**
     * 调用 release() 会让缓存把这个页面的引用计数减 1，归零时可能被驱逐
     */
    public void release() {
        pc.release(this);   // 通知缓存"我用完了"
    }

    /**
     * 当页面被修改时，调用 setDirty(true) 
     */
    public void setDirty(boolean dirty) {
        this.dirty = dirty;
    }

    /**
     * 缓存在驱逐或关闭时，会检查 isDirty()
     * 如果为 true 就写回磁盘
     * 这是延迟写回策略的基础
     */
    public boolean isDirty() {
        return dirty;
    }

    public int getPageNumber() {
        return pageNumber;
    }

    public byte[] getData() {
        return data;
    }

}
