package com.learning.sourcecode.curator;
// 基于curator实现的读写锁主要源码分析
// InterProcessReadWriteLock
// 读锁实现：
new SortingLockInternalsDriver()
{   
	// 读锁的实现和公平的独占锁流程大致一样，只是重写了其获取锁的方法
    @Override
    public PredicateResults getsTheLock(CuratorFramework client, List<String> children, String sequenceNodeName, int maxLeases) throws Exception
    {
        return readLockPredicate(children, sequenceNodeName);
    }
}

private PredicateResults readLockPredicate(List<String> children, String sequenceNodeName) throws Exception
{   
	// 如果当前线程已经获取写锁，可以再次获取读锁
    if ( writeMutex.isOwnedByCurrentThread() )
    {
        return new PredicateResults(null, true);
    }

    int         index = 0;
    int         firstWriteIndex = Integer.MAX_VALUE;
    int         ourIndex = Integer.MAX_VALUE;
    // 遍历子节点
    for ( String node : children )
    {    
    	// 判断是否有写锁节点
        if ( node.contains(WRITE_LOCK_NAME) )
        {
            firstWriteIndex = Math.min(index, firstWriteIndex);
        }
        // 找到读锁节点退出循环
        else if ( node.startsWith(sequenceNodeName) )
        {
            ourIndex = index;
            break;
        }

        ++index;
    }
    StandardLockInternalsDriver.validateOurIndex(sequenceNodeName, ourIndex);
    
    // 如果读锁节点在写锁节点之前，则获取锁，因为读锁共享
    boolean     getsTheLock = (ourIndex < firstWriteIndex);
    // getsTheLock为true，则不需要watcher监听，否则监听写锁节点
    String      pathToWatch = getsTheLock ? null : children.get(firstWriteIndex);
    return new PredicateResults(pathToWatch, getsTheLock);
}
// 写锁
writeMutex = new InternalInterProcessMutex
(
    client,
    basePath,
    WRITE_LOCK_NAME,
    1,
    new SortingLockInternalsDriver()
    {   
    	// 写锁，获取锁的策略还是根据排队来，因为独占互斥，排在第一位就可以获取锁，否则watcher前一个节点
        @Override
        public PredicateResults getsTheLock(CuratorFramework client, List<String> children, String sequenceNodeName, int maxLeases) throws Exception
        {
            return super.getsTheLock(client, children, sequenceNodeName, maxLeases);
        }
    }
);