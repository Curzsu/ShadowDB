package top.guoziyang.mydb.backend.dm.dataItem;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import top.guoziyang.mydb.backend.common.SubArray;
import top.guoziyang.mydb.backend.dm.DataManagerImpl;
import top.guoziyang.mydb.backend.dm.page.Page;

/**
 * dataItem 结构如下：
 * [ValidFlag] [DataSize] [Data]
 * ValidFlag 1字节，0为合法，1为非法
 * DataSize  2字节，标识Data的长度
 */
public class DataItemImpl implements DataItem {

    static final int OF_VALID = 0;  // 有效标志偏移量
    static final int OF_SIZE = 1;  // 大小字段偏移量
    static final int OF_DATA = 3;  // 数据内容偏移量

    private SubArray raw;   // 当前数据（指向页面的某个区域）
    private byte[] oldRaw;   // 修改前的数据备份（用于回滚）
    private Lock rLock;   // 读锁
    private Lock wLock;   // 写锁
    private DataManagerImpl dm;   // 数据管理器引用
    private long uid;   // 数据项的唯一标识（页号 + 偏移量）
    private Page pg;    // 所属页面

    public DataItemImpl(SubArray raw, byte[] oldRaw, Page pg, long uid, DataManagerImpl dm) {
        this.raw = raw;
        this.oldRaw = oldRaw;
        ReadWriteLock lock = new ReentrantReadWriteLock();
        rLock = lock.readLock();    // 允许多个读者
        wLock = lock.writeLock();    // 保证写操作的独占性（独占写入）
        this.dm = dm;
        this.uid = uid;
        this.pg = pg;
    }

    /**
     * 判断数据项是否有效
     * @return 数据项是否有效
     */
    public boolean isValid() {
        return raw.raw[raw.start+OF_VALID] == (byte)0;
    }

    @Override
    public SubArray data() {
        return new SubArray(raw.raw, raw.start+OF_DATA, raw.end);
    }

    /**
     * 获取数据项的原始数据
     * @return 数据项的原始数据
     */
    @Override
    public void before() {
        wLock.lock();   // 获取写锁
        pg.setDirty(true);   // 标记页面已修改（脏页）
        System.arraycopy(raw.raw, raw.start, oldRaw, 0, oldRaw.length);  // 备份旧数据
    }


    /**
     * 回滚到修改前的状态
     */
    @Override
    public void unBefore() {
        System.arraycopy(oldRaw, 0, raw.raw, raw.start, oldRaw.length); // 恢复旧数据
        wLock.unlock();
    }

    /**
     * 日志记录后释放写锁
     * @param xid 事务ID
     */
    @Override
    public void after(long xid) {
        dm.logDataItem(xid, this);   // 记录日志（新旧数据对比）
        wLock.unlock();   // 释放写锁
    }

    @Override
    public void release() {
        dm.releaseDataItem(this);
    }

    @Override
    public void lock() {
        wLock.lock();
    }

    @Override
    public void unlock() {
        wLock.unlock();
    }

    @Override
    public void rLock() {
        rLock.lock();
    }

    @Override
    public void rUnLock() {
        rLock.unlock();
    }

    @Override
    public Page page() {
        return pg;
    }

    @Override
    public long getUid() {
        return uid;
    }

    @Override
    public byte[] getOldRaw() {
        return oldRaw;
    }

    @Override
    public SubArray getRaw() {
        return raw;
    }
    
}
