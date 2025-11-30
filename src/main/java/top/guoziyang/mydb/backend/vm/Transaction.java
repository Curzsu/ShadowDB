package top.guoziyang.mydb.backend.vm;

import java.util.HashMap;
import java.util.Map;

import top.guoziyang.mydb.backend.tm.TransactionManagerImpl;

// vm对一个事务的抽象
public class Transaction {
    public long xid;
    public int level;
    public Map<Long, Boolean> snapshot;
    public Exception err;
    public boolean autoAborted;

    public static Transaction newTransaction(long xid, int level, Map<Long, Transaction> active) {
        Transaction t = new Transaction();
        t.xid = xid;
        t.level = level;    // 0: 读已提交，1: 可重复读

        // 如果是可重复读级别，那么需要记录快照
        if(level != 0) {
            // 把活跃事务的 XID 记录到快照中
            // 快照是一个 Map，记录了所有活跃事务的 XID
            t.snapshot = new HashMap<>();
            for(Long x : active.keySet()) {
                t.snapshot.put(x, true);
            }
        }
        return t;
    }

    public boolean isInSnapshot(long xid) {
        if(xid == TransactionManagerImpl.SUPER_XID) {
            return false;
        }
        return snapshot.containsKey(xid);
    }
}
