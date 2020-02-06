package com.learning.sourcecode.curator;// 基于curator实现分布式锁源码分析
InterProcessMutex
// 锁信息
private static class LockData
{   
    // 线程
    final Thread owningThread;
    // 节点路径
    final String lockPath;
    // 重入次数
    final AtomicInteger lockCount = new AtomicInteger(1);

    private LockData(Thread owningThread, String lockPath)
    {
        this.owningThread = owningThread;
        this.lockPath = lockPath;
    }
}
// 内部循环获取锁，关键函数
private boolean internalLockLoop(long startMillis, Long millisToWait, String ourPath) throws Exception
    {
        boolean     haveTheLock = false;
        boolean     doDelete = false;
        try
        {
            if ( revocable.get() != null )
            {
                client.getData().usingWatcher(revocableWatcher).forPath(ourPath);
            }

            while ( (client.getState() == CuratorFrameworkState.STARTED) && !haveTheLock )
            {   
                // 获取排序后的子节点
                List<String>        children = getSortedChildren();
                // 获取自己创建的临时顺序节点的名称
                String              sequenceNodeName = ourPath.substring(basePath.length() + 1); // +1 to include the slash
                
                // 去尝试获取锁
                PredicateResults    predicateResults = driver.getsTheLock(client, children, sequenceNodeName, maxLeases);
                if ( predicateResults.getsTheLock() )
                {   
                	// 获得锁，中断循环，继续返回上层
                    haveTheLock = true;
                }
                else
                {   
                	// 未获得锁，监听上一临时节点顺序
                    String  previousSequencePath = basePath + "/" + predicateResults.getPathToWatch();

                    synchronized(this)
                    {
                        try 
                        {
                            // use getData() instead of exists() to avoid leaving unneeded watchers which is a type of resource leak
                            // exists()会导致导致资源泄漏因此采用 getData()进行替换
                            // 上一临时顺序节点如果被删除，会唤醒当前线程继续竞争锁，正常情况下能直接获得锁，因为锁是公平的
                            client.getData().usingWatcher(watcher).forPath(previousSequencePath);
                            // 带有时间的等待和不带时间的等待
                            if ( millisToWait != null )
                            {
                                millisToWait -= (System.currentTimeMillis() - startMillis);
                                startMillis = System.currentTimeMillis();
                                if ( millisToWait <= 0 )
                                {
                                    doDelete = true;    // timed out - delete our node
                                    break;
                                }

                                wait(millisToWait);
                            }
                            else
                            {
                                wait();
                            }
                        }
                        catch ( KeeperException.NoNodeException e ) 
                        {
                            // it has been deleted (i.e. lock released). Try to acquire again
                        }
                    }
                }
            }
        }
        catch ( Exception e )
        {
            ThreadUtils.checkInterrupted(e);
            doDelete = true;
            throw e;
        }
        finally
        {
            if ( doDelete )
            {
                deleteOurPath(ourPath);
            }
        }
        return haveTheLock;
    }

public PredicateResults getsTheLock(CuratorFramework client, List<String> children, String sequenceNodeName, int maxLeases) throws Exception
{   
	// 临时节点在排序节点列表中的索引
    int             ourIndex = children.indexOf(sequenceNodeName);
    // 校验临时顺序节点是否有效
    validateOurIndex(sequenceNodeName, ourIndex);
    // 锁公平性的核心
    // 由InterProcessMutex的构造函数可知，maxLeases为 1，即只有ourIndex为0时，线程才能持有锁，或者说该线程创建的临时顺序节点激活了锁
    // Zookeeper的临时顺序节点特性能保证跨多个JVM的线程并发创建节点时的顺序性，越早创建临时顺序节点成功的线程会更早地激活锁或获得锁
    boolean         getsTheLock = ourIndex < maxLeases;
    // 如果已经获得了锁，则无需监听任何节点，否则需要监听上一顺序节点(ourIndex-1)因为锁是公平的，因此无需监听除了(ourIndex-1)以外的所有节点，这是为了减少惊群效应，非常巧妙的设计！！
    String          pathToWatch = getsTheLock ? null : children.get(ourIndex - maxLeases);
    // 如果获得了锁，则pathToWatch为null，getsTheLock为true
    // 否则pathToWatch为当前节点的上一节点的路径，getsTheLock为false
    return new PredicateResults(pathToWatch, getsTheLock);
}
// 释放锁的逻辑
public void release() throws Exception
{
    /*
        Note on concurrency: a given lockData instance
        can be only acted on by a single thread so locking isn't necessary
     */
    
    Thread      currentThread = Thread.currentThread();
    LockData    lockData = threadData.get(currentThread);
    // 如果不是当前线程持有锁，则抛出异常
    if ( lockData == null )
    {
        throw new IllegalMonitorStateException("You do not own the lock: " + basePath);
    }

    int newLockCount = lockData.lockCount.decrementAndGet();
    if ( newLockCount > 0 )
    {   
    	// 可重入性的体现，只有当newLockCount为0时，锁才释放
        return;
    }
    if ( newLockCount < 0 )
    {
        throw new IllegalMonitorStateException("Lock count has gone negative for lock: " + basePath);
    }
    try
    {    
    	// 释放锁
        internals.releaseLock(lockData.lockPath);
    }
    finally
    {    
    	// 最后从映射表中移除当前线程的锁信息
        threadData.remove(currentThread);
    }
}

private void deleteOurPath(String ourPath) throws Exception
{
    try
    {
    	// 删除临时顺序节点，只会触发后一顺序节点去获取锁，理论上不存在竞争，只排队，非抢占，公平锁，先到先得
        client.delete().guaranteed().forPath(ourPath);
    }
    catch ( KeeperException.NoNodeException e )
    {
        // ignore - already deleted (possibly expired session, etc.)
    }
}
// 在释放锁，会删除当前节点，从而触发watcher机制，客户端会调用如下函数
private final Watcher watcher = new Watcher()
{
    @Override
    public void process(WatchedEvent event)
    {
        notifyFromWatcher();
    }
};
// 这样达到唤醒的目的
private synchronized void notifyFromWatcher()
{
    notifyAll();
}
