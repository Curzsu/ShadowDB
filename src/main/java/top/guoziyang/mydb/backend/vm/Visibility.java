package top.guoziyang.mydb.backend.vm;

import top.guoziyang.mydb.backend.tm.TransactionManager;

public class Visibility {
    
    public static boolean isVersionSkip(TransactionManager tm, Transaction t, Entry e) {
        long xmax = e.getXmax();
        if(t.level == 0) {
            return false;
        } else {
            return tm.isCommitted(xmax) && (xmax > t.xid || t.isInSnapshot(xmax));
        }
    }

    public static boolean isVisible(TransactionManager tm, Transaction t, Entry e) {
        if(t.level == 0) {
            return readCommitted(tm, t, e);
        } else {
            return repeatableRead(tm, t, e);
        }
    }

    // 读已提交隔离级别【即只能读到别人已经 commit 了的数据】
    // t: 当前正在读取的事务
    // e: 数据库里的一行数据(Entry)
    private static boolean readCommitted(TransactionManager tm, Transaction t, Entry e) {
        long xid = t.xid;   // 当前正在读取的事务ID
        long xmin = e.getXmin();   // 创建该条数据的事务ID
        long xmax = e.getXmax();   // 删除该条数据的事务ID（如果是0，说明还没被删除）

        // 1. 如果当前正在读取的事务ID由我创建，且还没被删除 => 可见
        if(xmin == xid && xmax == 0) return true;

        // 2. 如果是别人创建的，首先那个人必须已经提交了(Committed)
        if(tm.isCommitted(xmin)) {
            // 2.1 如果还没被删除 => 可见
            if(xmax == 0) return true;

            // 2.2 如果有人删了(xmax != 0)，但删它的那个人不是我
            if(xmax != xid) {
                // 2.3 且删它的那个人还没提交 => 对我来说它还没删 => 可见
                if(!tm.isCommitted(xmax)) {
                    return true;
                }
            }
        }
        // 其他情况都不可见
        return false;
    }

    private static boolean repeatableRead(TransactionManager tm, Transaction t, Entry e) {
        long xid = t.xid;   // 当前事务ID (我)
        long xmin = e.getXmin();   // 创建该条数据的事务ID
        long xmax = e.getXmax();   // 删除该条数据的事务ID（如果是0，说明还没被删除）

        // 1. 如果该条数据由我创建，且还没被删除 => 可见
        if(xmin == xid && xmax == 0) return true;

        // 必须满足三个条件才能“看见”：
    // A. tm.isCommitted(xmin): 这个事务在全局状态下必须是“已提交”的
    // B. xmin < xid: 创建它的事务必须比我“老”。如果 xmin > xid，说明是我出生后它才出生的，那是“未来数据”，我不看
    // C. !t.isInSnapshot(xmin): 关键点！它不能在我的“快照”里。如果已经在快照里，说明我出生时它还没提交，所以我不能认
        if(tm.isCommitted(xmin) && xmin < xid && !t.isInSnapshot(xmin)) {
            if(xmax == 0) return true;  // 如果还没被删除 => 可见

            // 2. 如果有人删了(xmax != 0)，但删它的那个人不是我
            if(xmax != xid) {
                // 如果删除它的事务：
                // A. 还没提交 (!isCommitted)
                // B. 或者是在我出生之后才出生的 (xmax > xid)
                // C. 或者是虽然比我老，但我出生时它还没提交 (isInSnapshot)
                // 满足任一条件，说明这个删除动作对我来说“不存在” -> 依然可见！
                if(!tm.isCommitted(xmax) || xmax > xid || t.isInSnapshot(xmax)) {
                    return true;
                }
            }
        }
        return false;
    }

}
