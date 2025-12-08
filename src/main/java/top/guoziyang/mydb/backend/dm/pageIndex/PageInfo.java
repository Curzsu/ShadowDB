package top.guoziyang.mydb.backend.dm.pageIndex;

/**
 * PageInfo 类用于存储页面编号和空闲空间信息
 * @author Lukesu
 * @date 2025-12-08
 * @version 1.0
 */

public class PageInfo {
    public int pgno;
    public int freeSpace;

    public PageInfo(int pgno, int freeSpace) {
        this.pgno = pgno;
        this.freeSpace = freeSpace;
    }
}
