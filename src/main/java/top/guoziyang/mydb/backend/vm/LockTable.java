package top.guoziyang.mydb.backend.vm;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import top.guoziyang.mydb.common.Error;

/**
 * 维护了一个依赖等待图，以进行死锁检测
 */
public class LockTable {
    
    private Map<Long, List<Long>> x2u;  // 某个XID已经获得的资源的UID列表
    private Map<Long, Long> u2x;        // UID被某个XID持有
    private Map<Long, List<Long>> wait; // 正在等待UID的XID列表
    private Map<Long, Lock> waitLock;   // 正在等待资源的XID的锁
    private Map<Long, Long> waitU;      // XID正在等待的UID
    private Lock lock;

    
    public LockTable() {
        x2u = new HashMap<>(); 
        u2x = new HashMap<>();
        wait = new HashMap<>();
        waitLock = new HashMap<>();
        waitU = new HashMap<>();
        lock = new ReentrantLock();
    }

    // 不需要等待则返回null，否则返回锁对象
    // 会造成死锁则抛出异常
    public Lock add(long xid, long uid) throws Exception {
        lock.lock(); 
        try {
            if(isInList(x2u, xid, uid)) { 
                return null; 
            }
            if(!u2x.containsKey(uid)) {
                u2x.put(uid, xid);
                putIntoList(x2u, xid, uid);
                return null;
            }

             // ... 如果被别人锁了 ...
            waitU.put(xid, uid);    // 记录我在等谁
            //putIntoList(wait, xid, uid);
            putIntoList(wait, uid, xid);

            // 【关键】每次加锁阻塞前，先检查会不会构成环
            if(hasDeadLock()) {

                // 如果构成环，则回滚，也就是撤销刚才的等待操作
                waitU.remove(xid);
                removeFromList(wait, uid, xid);

                // 抛出异常，让事务回滚
                throw Error.DeadlockException;
            }
            Lock l = new ReentrantLock();
            l.lock();
            waitLock.put(xid, l);
            return l;

        } finally {
            lock.unlock();
        }
    }

    public void remove(long xid) {
        lock.lock();
        try {
            List<Long> l = x2u.get(xid);
            if(l != null) {
                while(l.size() > 0) {
                    Long uid = l.remove(0);
                    selectNewXID(uid);
                }
            }
            waitU.remove(xid);
            x2u.remove(xid);
            waitLock.remove(xid);

        } finally {
            lock.unlock();
        }
    }

    // 从等待队列中选择一个xid来占用uid
    private void selectNewXID(long uid) {
        u2x.remove(uid);
        List<Long> l = wait.get(uid);
        if(l == null) return;
        assert l.size() > 0;

        while(l.size() > 0) {
            long xid = l.remove(0);
            if(!waitLock.containsKey(xid)) {
                continue;
            } else {
                u2x.put(uid, xid);
                Lock lo = waitLock.remove(xid);
                waitU.remove(xid);
                lo.unlock();
                break;
            }
        }

        if(l.size() == 0) wait.remove(uid);
    }

    private Map<Long, Integer> xidStamp;    // 记录每个XID的访问时间戳
    private int stamp;  // 当前遍历的批次号

    private boolean hasDeadLock() {
        xidStamp = new HashMap<>();
        stamp = 1;

        // 遍历所有持有资源的事务（也就是图中的所有起点）
        for(long xid : x2u.keySet()) {
            Integer s = xidStamp.get(xid);

            // 优化：如果这个节点在之前的 dfs 中已经作为“非环节点”被访问过了，就跳过，避免重复计算
            if(s != null && s > 0) {
                continue;
            }
            stamp ++;   // 开启新一轮的搜索
            if(dfs(xid)) {
                return true;    // 只要找到一个环，立马报错返回
            }
        }
        return false;
    }

    private boolean dfs(long xid) {
        Integer stp = xidStamp.get(xid);

        // 【情况1：找到环了！】
        // 如果当前节点的时间戳 == 当前轮次(stamp)，说明我们在这一轮搜索中，
        // 转了一圈又回到了这个点。
        // 比如：A(stamp=5) -> B(stamp=5) -> A(stamp=5)
        if(stp != null && stp == stamp) {
            return true;
        }


        // 【情况2：遇到老节点】
        // 如果这个点的时间戳 < 当前轮次，说明它在以前的轮次被检查过，且没有发现环。
        // 既然以前没环，现在走到这也肯定没环，直接返回 false
        if(stp != null && stp < stamp) {
            return false;
        }


        // 【情况3：新节点】
        // 暂时标记为当前轮次，表示“我正在访问这个链条”
        xidStamp.put(xid, stamp);


        // --- 寻找下一个节点 ---

        // 1. 我(xid)在等哪个资源(uid)？
        Long uid = waitU.get(xid);

        // 如果我没在等任何资源，说明我是链条的终点，没环
        if(uid == null) return false;
        Long x = u2x.get(uid);

        // 这里 assert x != null 是因为如果我在等uid，那uid肯定被别人拿着
        assert x != null;

        // 3. 递归去查那个拿着资源的人
        return dfs(x);
    }

    private void removeFromList(Map<Long, List<Long>> listMap, long uid0, long uid1) {
        List<Long> l = listMap.get(uid0);
        if(l == null) return;
        Iterator<Long> i = l.iterator();
        while(i.hasNext()) {
            long e = i.next();
            if(e == uid1) {
                i.remove();
                break;
            }
        }
        if(l.size() == 0) {
            listMap.remove(uid0);
        }
    }

    private void putIntoList(Map<Long, List<Long>> listMap, long uid0, long uid1) {
        if(!listMap.containsKey(uid0)) {
            listMap.put(uid0, new ArrayList<>());
        }
        listMap.get(uid0).add(0, uid1);
    }

    private boolean isInList(Map<Long, List<Long>> listMap, long uid0, long uid1) {
        List<Long> l = listMap.get(uid0);
        if(l == null) return false;
        Iterator<Long> i = l.iterator();
        while(i.hasNext()) {
            long e = i.next();
            if(e == uid1) {
                return true;
            }
        }
        return false;
    }

}
