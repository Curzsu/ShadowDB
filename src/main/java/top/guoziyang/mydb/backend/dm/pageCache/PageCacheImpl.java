package top.guoziyang.mydb.backend.dm.pageCache;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import top.guoziyang.mydb.backend.common.AbstractCache;
import top.guoziyang.mydb.backend.dm.page.Page;
import top.guoziyang.mydb.backend.dm.page.PageImpl;
import top.guoziyang.mydb.backend.utils.Panic;
import top.guoziyang.mydb.common.Error;

public class PageCacheImpl extends AbstractCache<Page> implements PageCache {
    
    private static final int MEM_MIN_LIM = 10; // 最小缓存10页
    public static final String DB_SUFFIX = ".db"; // 数据库文件后缀

    // RandomAccessFile: 支持随机读写，可以直接跳到指定位置
    private RandomAccessFile file;  // 随机访问文件对象
    private FileChannel fc; // NIO文件通道，更高效的IO
    private Lock fileLock;  // 文件锁, 保证并发安全

    // AtomicInteger: 线程安全的计数器
    private AtomicInteger pageNumbers;  // 原子整数，记录当前页面总数

    PageCacheImpl(RandomAccessFile file, FileChannel fileChannel, int maxResource) {
        super(maxResource);
        if(maxResource < MEM_MIN_LIM) {
            Panic.panic(Error.MemTooSmallException);
        }
        long length = 0;
        try {
            length = file.length();
        } catch (IOException e) {
            Panic.panic(e);
        }
        this.file = file;
        this.fc = fileChannel;
        this.fileLock = new ReentrantLock();
        this.pageNumbers = new AtomicInteger((int)length / PAGE_SIZE);
    }

    /**
     * 创建新页面
     * 流程：
     * 调用newPage() → 页号+1 → 创建Page对象 → 立即刷盘 → 返回页号
     * @param initData 页面初始化数据
     * @return 新页面的页号
     */
    public int newPage(byte[] initData) {
        int pgno = pageNumbers.incrementAndGet(); // 原子递增，线程安全
        Page pg = new PageImpl(pgno, initData, null);
        flush(pg);  // 立即写入磁盘
        return pgno;    // 返回页号
    }   

    /**
     * 根据pageNumber获取页面
     * @param pgno 页面编号
     * @return 页面对象
     */
    public Page getPage(int pgno) throws Exception {
        return get((long)pgno);
    }

    /**
     * 根据pageNumber从数据库文件中读取页数据，并包裹成Page
     */
    @Override
    protected Page getForCache(long key) throws Exception {
        int pgno = (int)key;

        // pageOffset(pgno) = (pgno-1) * 8192，计算页在文件中的字节偏移
        long offset = PageCacheImpl.pageOffset(pgno);   

        ByteBuffer buf = ByteBuffer.allocate(PAGE_SIZE);    // 分配一个PAGE_SIZE（8KB）大小的ByteBuffer
        fileLock.lock();    // 加锁，保证并发安全
        try {
            fc.position(offset);    // 定位到文件位置
            fc.read(buf);    // 从文件中读取数据(8KB)到ByteBuffer
        } catch(IOException e) {
            Panic.panic(e);
        }
        fileLock.unlock();
        return new PageImpl(pgno, buf.array(), this);
    }

    /**
     * 释放页面
     * 流程：
     * 调用release() → 判断是否被修改 → 如果被修改则写回磁盘 → 清除脏标记
     * 注意：内存中被修改但还未写回磁盘的页面，必须先写盘才能释放
     * @param pg 页面对象
     */
    @Override
    protected void releaseForCache(Page pg) {
        if(pg.isDirty()) {  // 如果页面被修改过
            flush(pg);  // 写回磁盘
            pg.setDirty(false);  // 清除脏标记
        }
    }

    public void release(Page page) {
        release((long)page.getPageNumber());
    }

    public void flushPage(Page pg) {
        flush(pg);
    }

    /**
     * 磁盘IO操作：
     * 将页面写回磁盘
     * @param pg 页面对象
     */
    private void flush(Page pg) {
        int pgno = pg.getPageNumber();
        long offset = pageOffset(pgno);

        fileLock.lock();
        try {
            ByteBuffer buf = ByteBuffer.wrap(pg.getData()); // 包装字节数组
            fc.position(offset);    // 定位
            fc.write(buf);    // 将ByteBuffer中的数据写入文件
            fc.force(false);    // 强制刷新到磁盘（关键！）
        } catch(IOException e) {
            Panic.panic(e);
        } finally {
            fileLock.unlock();
        }
    }

    /**
     * 作用：回滚事务时删除无效页面
     * 截断数据库文件
     * @param maxPgno 截断到的最大页号
     */
    public void truncateByBgno(int maxPgno) {
        long size = pageOffset(maxPgno + 1);
        try {
            file.setLength(size);
        } catch (IOException e) {
            Panic.panic(e);
        }
        pageNumbers.set(maxPgno);
    }

    @Override
    public void close() {
        super.close();
        try {
            fc.close();
            file.close();
        } catch (IOException e) {
            Panic.panic(e);
        }
    }

    public int getPageNumber() {
        return pageNumbers.intValue();
    }

    /**
     * 根据页号计算页在文件中的字节偏移
     * @param pgno 页号
     * @return 页在文件中的字节偏移
     */
    private static long pageOffset(int pgno) {
        return (pgno-1) * PAGE_SIZE;  // 页号从1开始，第1页偏移=0
    }
    
}
