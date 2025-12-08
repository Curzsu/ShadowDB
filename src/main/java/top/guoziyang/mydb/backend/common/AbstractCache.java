package top.guoziyang.mydb.backend.common;

import java.util.HashMap;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import top.guoziyang.mydb.common.Error;

/**
 * AbstractCache 实现了一个引用计数策略的缓存
 * 
 * 核心思路：
 * 数据库系统中,同一个资源(比如页面)可能被多个事务或操作同时使用。
 * 如果用传统的 LRU 缓存,可能会驱逐掉正在被使用的资源,导致数据不一致。
 * 解决方案: 引用计数：只要有人在用这个资源,就不能被驱逐;
 * 只有引用计数降为 0 时,才真正释放。
 * 
 * 待改进：
 * 1. 缓存满时不友好, 直接抛 CacheFullException,没有实现 LRU 等驱逐策略
 * 2. 自旋等待效率低, getting 检查用的是 sleep(1ms) 自旋,高并发下可能有性能问题
 * 3. 必须手动调用 release, 如果忘了就会泄漏(引用计数不归零)
 * 
 * @author Lukesu
 * @version 0.1
 * @date 2025-12-08
 */

public abstract class AbstractCache<T> {
    private HashMap<Long, T> cache;                     // 实际缓存的数据
    private HashMap<Long, Integer> references;          // 元素的引用个数
    private HashMap<Long, Boolean> getting;             // 正在获取某资源的线程

    private int maxResource;                            // 缓存的最大缓存资源数
    private int count = 0;                              // 缓存中元素的个数
    private Lock lock;                                  // 保护缓存的锁

    public AbstractCache(int maxResource) { 
        this.maxResource = maxResource; 
        cache = new HashMap<>();
        references = new HashMap<>();
        getting = new HashMap<>();  
        lock = new ReentrantLock(); 
    }

    /**
     * 如果资源正在被另一个线程从磁盘加载,当前线程就睡 1ms 再检查。这避免了重复加载同一资源。
     */
    protected T get(long key) throws Exception { 
        while(true) {
            lock.lock();
            if(getting.containsKey(key)) {
                // 请求的资源正在被其他线程获取
                lock.unlock();
                try {
                    Thread.sleep(1);       // 自旋等待
                } catch (InterruptedException e) { 
                    e.printStackTrace();    
                    continue;
                }
                continue;
            }


            /**
             * 缓存命中，直接返回
             */
            if(cache.containsKey(key)) {
                T obj = cache.get(key);
                references.put(key, references.get(key) + 1); // 引用计数加一,意味多了一个地方在使用它
                lock.unlock();
                return obj;
            }

            // 缓存已满且无法再加载新资源
            if(maxResource > 0 && count == maxResource) { 
                lock.unlock();  
                throw Error.CacheFullException;  // 缓存满了,直接报错
            }

            //如果没满
            count ++; // 资源个数加一
            getting.put(key, true); // 就在 getting 里做个标记,告诉其他线程"这个资源我来加载,你们别重复加载"
            lock.unlock();
            break;
        }


        /**
         * 缓存未命中，需要从磁盘加载
         * 调用抽象方法 getForCache,让子类去实现具体的加载逻辑(比如从磁盘读页面)
         * 如果失败了,要清理之前的标记
         */
        T obj = null;
        try {
            obj = getForCache(key); // 调用子类实现,从磁盘读取
        } catch(Exception e) {
            lock.lock();
            count --;
            getting.remove(key);  // 加载失败,回滚状态
            lock.unlock();
            throw e;
        }


        /**
         * 加载成功,将资源放入缓存
         */
        lock.lock();
        getting.remove(key);    // 移除"正在加载"标记
        cache.put(key, obj);    // 将资源放入缓存
        references.put(key, 1);   // 引用计数设为 1 (因为当前调用者在使用)
        lock.unlock();
        
        return obj;
    }

    /**
     * 强行释放一个缓存
     * 每次 release 都让引用计数减 1
     * 只有计数归零时,才调用 releaseForCache 做真正的清理(比如脏页写回)
     * 这保证了正在使用的资源绝不会被驱逐
     */
    protected void release(long key) {
        lock.lock();
        try {
            int ref = references.get(key)-1;    // 引用计数减一

            // 引用计数归零,真正释放资源
            if(ref == 0) {  
                T obj = cache.get(key);
                releaseForCache(obj);   // 调用子类实现,比如写回磁盘
                references.remove(key); // 移除引用计数
                cache.remove(key);    // 从缓存中移除资源
                count --;
            } else {    
                references.put(key, ref);   // 还有人在用,只减计数
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * 关闭缓存，写回所有资源
     */
    protected void close() {
        lock.lock();
        try {
            Set<Long> keys = cache.keySet();
            for (long key : keys) {
                T obj = cache.get(key);
                releaseForCache(obj);
                references.remove(key);
                cache.remove(key);
            }
        } finally {
            lock.unlock();
        }
    }


    /**
     * 当资源不在缓存时的获取行为
     */
    protected abstract T getForCache(long key) throws Exception;
    /**
     * 当资源被驱逐时的写回行为
     */
    protected abstract void releaseForCache(T obj);
}
