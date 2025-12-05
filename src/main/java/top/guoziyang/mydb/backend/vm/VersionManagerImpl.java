package top.guoziyang.mydb.backend.vm;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import top.guoziyang.mydb.backend.common.AbstractCache;
import top.guoziyang.mydb.backend.dm.DataManager;
import top.guoziyang.mydb.backend.tm.TransactionManager;
import top.guoziyang.mydb.backend.tm.TransactionManagerImpl;
import top.guoziyang.mydb.backend.utils.Panic;
import top.guoziyang.mydb.common.Error;

public class VersionManagerImpl extends AbstractCache<Entry> implements VersionManager {

    TransactionManager tm;
    DataManager dm;
    Map<Long, Transaction> activeTransaction;
    Lock lock;
    LockTable lt;

    public VersionManagerImpl(TransactionManager tm, DataManager dm) {
        super(0);
        this.tm = tm;
        this.dm = dm;
        this.activeTransaction = new HashMap<>();
        activeTransaction.put(TransactionManagerImpl.SUPER_XID, Transaction.newTransaction(TransactionManagerImpl.SUPER_XID, 0, null));
        this.lock = new ReentrantLock();
        this.lt = new LockTable();
    }

    @Override
    public byte[] read(long xid, long uid) throws Exception {
        lock.lock();
        Transaction t = activeTransaction.get(xid);
        lock.unlock();

        if(t.err != null) {
            throw t.err;
        }

        Entry entry = null;
        try {
            entry = super.get(uid);
        } catch(Exception e) {
            if(e == Error.NullEntryException) {
                return null;
            } else {
                throw e;
            }
        }
        try {
            if(Visibility.isVisible(tm, t, entry)) {
                return entry.data();
            } else {
                return null;
            }
        } finally {
            entry.release();
        }
    }

    @Override
    public long insert(long xid, byte[] data) throws Exception {
        lock.lock();
        Transaction t = activeTransaction.get(xid);
        lock.unlock();

        if(t.err != null) {
            throw t.err;
        }

        byte[] raw = Entry.wrapEntryRaw(xid, data);
        return dm.insert(xid, raw);
    }

    @Override
    public boolean delete(long xid, long uid) throws Exception {
        lock.lock();
        Transaction t = activeTransaction.get(xid);
        lock.unlock();

        if(t.err != null) {
            throw t.err;
        }
        Entry entry = null;
        try {
            entry = super.get(uid);
        } catch(Exception e) {
            if(e == Error.NullEntryException) {
                return false;
            } else {
                throw e;
            }
        }
        try {
            // 1. 检查可见性：如果我都看不见这条数据，自然不能删它
            if(!Visibility.isVisible(tm, t, entry)) {
                return false;
            }

            Lock l = null;
            try {
                // 【增长阶段：加锁】
                // 2. 这里的 lt 是 LockTable，防止并发修改同一条数据（写写冲突还是要加锁的）
                // MVCC 解决的是 读写冲突，写写冲突依然需要锁，所以这里仍需要锁，加锁防止并发修改同一条数据
                // 【动作 1】去 LockTable 登记，我要占领 uid=88
                // 如果返回 null，说明占领成功，不用等。
                // 如果返回 Lock 对象，说明被占了，我就在这里卡住（等待）或者报错（死锁）。
                l = lt.add(xid, uid); 
            } catch(Exception e) {
                t.err = Error.ConcurrentUpdateException; 
                internAbort(xid, true); 
                t.autoAborted = true; 
                throw t.err;  
            }
 
            // 注意：这里拿到锁(l)后，并没有代码去释放它！
            // 这里的 l.lock/unlock 只是为了利用 Lock 对象做线程阻塞，
            // 并不是释放 LockTable 里的逻辑占用。
            // 【动作 2】如果拿到号牌（Lock对象），说明我要排队
            if(l != null) {
                l.lock();   // 试图锁住号牌，这一步会让我（当前线程）“睡过去”
                l.unlock(); // 等我醒来（说明前面的人把资源给我了），我把号牌销毁，继续干活
            }
   
            if(entry.getXmax() == xid) {
                return false;
            }

            if(Visibility.isVersionSkip(tm, t, entry)) {
                t.err = Error.ConcurrentUpdateException;
                internAbort(xid, true);
                t.autoAborted = true;
                throw t.err;
            }

            // 3. 核心动作：将 XMAX 设置为当前事务ID，标记删除
            // 这里是执行真正的删除操作（设置 XMAX），而不是简单标记。
            entry.setXmax(xid);

            // 方法结束了，但我依然霸占着 uid 这个资源，没有调用 lt.remove！
            // 注意：这里没有释放 uid=88！我的 x2u 列表里多了一个 88
            // 这就是“增长”！
            return true;

        } finally {
            entry.release();
        }
    }

    @Override
    public long begin(int level) {
        lock.lock();
        try {
            long xid = tm.begin();  // 向 TM 申请一个新的 XID，作为当前事务ID

            // 根据隔离级别创建事务对象，传入 activeTransaction (当前所有活跃事务的Map)
            Transaction t = Transaction.newTransaction(xid, level, activeTransaction);  

            // 把自己也加入活跃列表，方便后续的可见性检查
            activeTransaction.put(xid, t);
            return xid;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void commit(long xid) throws Exception {
        lock.lock();
        Transaction t = activeTransaction.get(xid);
        lock.unlock();

        try {
            if(t.err != null) {
                throw t.err;
            }
        } catch(NullPointerException n) {
            System.out.println(xid);
            System.out.println(activeTransaction.keySet());
            Panic.panic(n);
        }

        lock.lock();
        activeTransaction.remove(xid);
        lock.unlock();

        lt.remove(xid);
        tm.commit(xid);
    }

    @Override
    public void abort(long xid) {
        internAbort(xid, false);
    }

    private void internAbort(long xid, boolean autoAborted) {
        lock.lock();
        Transaction t = activeTransaction.get(xid);
        if(!autoAborted) {
            activeTransaction.remove(xid);
        }
        lock.unlock();

        if(t.autoAborted) return;
        lt.remove(xid);
        tm.abort(xid);
    }

    public void releaseEntry(Entry entry) {
        super.release(entry.getUid());
    }

    @Override
    protected Entry getForCache(long uid) throws Exception {
        Entry entry = Entry.loadEntry(this, uid);
        if(entry == null) {
            throw Error.NullEntryException;
        }
        return entry;
    }

    @Override
    protected void releaseForCache(Entry entry) {
        entry.remove();
    }
    
}
