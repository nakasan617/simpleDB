package simpledb;

import java.io.*;
import java.util.*;

/**
 * Manages locks on PageIds held by TransactionIds.
 * S-locks and X-locks are represented as Permissions.READ_ONLY and Permisions.READ_WRITE, respectively
 *
 * All the field read/write operations are protected by this
 */
public class LockManager {

    final int LOCK_WAIT = 10;       // milliseconds
    HashMap<PageId, HashSet<TransactionId>> rLocks;
    HashMap<PageId, TransactionId> wLocks;
    HashMap<TransactionId, HashSet<PageId>> rLockReverseIndex;
    HashMap<TransactionId, HashSet<PageId>> wLockReverseIndex;

    /**
     * Sets up the lock manager to keep track of page-level locks for transactions
     * Should initialize state required for the lock table data structure(s)
     */
    public LockManager() {
        rLocks = new HashMap<PageId, HashSet<TransactionId>> ();
        wLocks = new HashMap<PageId, TransactionId> ();
        rLockReverseIndex = new HashMap<TransactionId, HashSet<PageId>> ();
        wLockReverseIndex = new HashMap<TransactionId, HashSet<PageId>> ();
    }

    /**
     * Tries to acquire a lock on page pid for transaction tid, with permissions perm.
     * If cannot acquire the lock, waits for a timeout period, then tries again.
     * This method does not return until the lock is granted, or an exception is thrown
     *
     * In Exercise 5, checking for deadlock will be added in this method
     * Note that a transaction should throw a DeadlockException in this method to
     * signal that it should be aborted.
     *
     * @throws DeadlockException after on cycle-based deadlock
     */
    public boolean acquireLock(TransactionId tid, PageId pid, Permissions perm)
            throws DeadlockException {
        int cnt = 0;
        while(!lock(tid, pid, perm)) { // keep trying to get the lock
            synchronized(this) {
                // you don't have the lock yet
                // possibly some code here for Exercise 5, deadlock detection
            }

            try {
                // couldn't get lock, wait for some time, then try again
                Thread.sleep(LOCK_WAIT);
            } catch (InterruptedException e) { // do nothing
            }

        }

        synchronized(this) {
            // for Exercise 5, might need some cleanup on deadlock detection data structure
        }

        return true;
    }

    /**
     * Release all locks corresponding to TransactionId tid.
     * This method is used by BufferPool.transactionComplete()
     */
    public synchronized void releaseAllLocks(TransactionId tid) {
        if(rLockReverseIndex.containsKey(tid)) {
            HashSet<PageId> pageIds = rLockReverseIndex.get(tid);
            rLockReverseIndex.remove(tid); // removing the reverse index here
            Iterator<PageId> it = pageIds.iterator();
            while(it.hasNext()) {
                PageId next = it.next();
                HashSet<TransactionId> tids = rLocks.get(next);
                tids.remove(tid);
                // removing the readLocks below
                if(tids.size() > 0) {
                    rLocks.put(next, tids);
                } else {
                    rLocks.remove(next);
                }
            }
        }

        if(wLockReverseIndex.containsKey(tid)) {
            HashSet<PageId> pageIds = wLockReverseIndex.get(tid);
            wLockReverseIndex.remove(tid);
            Iterator<PageId> it = pageIds.iterator();
            while(it.hasNext()) {
                PageId next = it.next();
                if(wLocks.containsKey(next)) {
                    wLocks.remove(next);
                } else {
                    System.out.println("locks not found here");
                }
            }
        }

    }

    /** Return true if the specified transaction has a lock on the specified page */
    public synchronized boolean holdsLock(TransactionId tid, PageId p) {
        if(rLocks.containsKey(p) && rLocks.get(p).contains(tid)) {
            return true;
        } else if(wLocks.containsKey(p) && wLocks.get(p) == tid) {
            return true;
        } else {
            return false;
        }
    }

    /**
     * Answers the question: is this transaction "locked out" of acquiring lock on this page with this perm?
     * Returns false if this tid/pid/perm lock combo can be achieved (i.e., not locked out), true otherwise.
     *
     * Logic:
     *
     * if perm == READ_ONLY
     *  if tid is holding any sort of lock on pid, then the tid can acquire the lock (return false).
     *
     *  if another tid is holding a READ lock on pid, then the tid can acquire the lock (return false).
     *  if another tid is holding a WRITE lock on pid, then tid can not currently
     *  acquire the lock (return true).
     *
     * else
     *   if tid is THE ONLY ONE holding a READ lock on pid, then tid can acquire the lock (return false).
     *   if tid is holding a WRITE lock on pid, then the tid already has the lock (return false).
     *
     *   if another tid is holding any sort of lock on pid, then the tid cannot currenty acquire the lock (return true).
     */
    private synchronized boolean locked(TransactionId tid, PageId pid, Permissions perm) {
        if(perm == Permissions.READ_ONLY) {

            if(!wLocks.containsKey(pid) || wLocks.get(pid) == tid) {
                return false;
            } else { // wLocks.containsKey(pid) && wLocks.get(pid) != tid
                return true;
            }
        } else {

            // let's see if it is fine from the writeLock perspective
            if (wLocks.containsKey(pid) && wLocks.get(pid) != tid) {
                return true;
            } else {
                // let's see if it is fine from the readLock perspective
                if(!rLocks.containsKey(pid)) {
                    return false;
                } else {
                    // rLock.containsKey(pid) == true
                    if(rLocks.get(pid).contains(tid) && rLocks.get(pid).size() == 1) {
                        return false;
                    } else {
                        return true;
                    }
                }
            }
        }
    }

    /**
     * Releases whatever lock this transaction has on this page
     * Should update lock table data structure(s)
     *
     * Note that you do not need to "wake up" another transaction that is waiting for a lock on this page,
     * since that transaction will be "sleeping" and will wake up and check if the page is available on its own
     * However, if you decide to change the fact that a thread is sleeping in acquireLock(), you would have to wake it up here
     */
    public synchronized void releaseLock(TransactionId tid, PageId pid) {
        if(wLocks.containsKey(pid)) {
            assert wLocks.get(pid) == tid && wLockReverseIndex.containsKey(tid);
            wLocks.remove(pid);
            wLockReverseIndex.get(tid).remove(pid);
            if(wLockReverseIndex.get(tid).size() == 0) {
                wLockReverseIndex.remove(tid);
            }
        }

        if(rLocks.containsKey(pid)) {
            assert rLocks.get(pid).contains(tid) && rLockReverseIndex.containsKey(tid);
            rLocks.get(pid).remove(tid);
            if(rLocks.get(pid).size() == 0) {
                rLocks.remove(pid);
            }
            rLockReverseIndex.get(tid).remove(pid);
            if(rLockReverseIndex.get(tid).size() == 0) {
                rLockReverseIndex.remove(tid);
            }
        }

    }

    /**
     * Attempt to lock the given PageId with the given Permissions for this TransactionId
     * Should update the lock table data structure(s) if successful
     *
     * Returns true if the lock attempt was successful, false otherwise
     */
    private synchronized boolean lock(TransactionId tid, PageId pid, Permissions perm) {

        if(locked(tid, pid, perm)) {
            return false; // this transaction cannot get the lock on this page; it is "locked out"
        }

        // Else, this transaction is able to get the lock, update lock table
        if(perm == Permissions.READ_ONLY) {
            if(rLocks.containsKey(pid)) {
                HashSet<TransactionId> tids = rLocks.get(pid);
                if(tids.add(tid)) {
                    rLocks.put(pid, tids);
                }
            } else {
                HashSet<TransactionId> tids = (new HashSet<TransactionId>());
                if(tids.add(tid)) {
                    rLocks.put(pid, tids);
                }
            }

            if(rLockReverseIndex.containsKey(tid)) {
                HashSet<PageId> pids = rLockReverseIndex.get(tid);
                if(pids.add(pid)) {
                    rLockReverseIndex.put(tid, pids);
                }
            } else {
                HashSet<PageId> pids = (new HashSet<PageId> ());
                if(pids.add(pid)) {
                    rLockReverseIndex.put(tid, pids);
                }
            }
        } else {
            // perm == READ_WRITE
            if(rLocks.containsKey(pid)) {
                HashSet<TransactionId> tids = rLocks.get(pid);
                if(tids.add(tid)) {
                    rLocks.put(pid, tids);
                }
            } else {
                HashSet<TransactionId> tids = (new HashSet<TransactionId>());
                if(tids.add(tid)) {
                    rLocks.put(pid, tids);
                }
            }

            if(rLockReverseIndex.containsKey(tid)) {
                HashSet<PageId> pids = rLockReverseIndex.get(tid);
                if(pids.add(pid)) {
                    rLockReverseIndex.put(tid, pids);
                }
            } else {
                HashSet<PageId> pids = (new HashSet<PageId> ());
                if(pids.add(pid)) {
                    rLockReverseIndex.put(tid, pids);
                }
            }

            if(wLocks.containsKey(pid)) {
                assert(wLocks.get(pid) == tid);
            } else {
                wLocks.put(pid, tid);
            }

            if(wLockReverseIndex.containsKey(tid)) {
                HashSet<PageId> pids = wLockReverseIndex.get(tid);
                if(pids.add(pid)) {
                    wLockReverseIndex.put(tid, pids);
                }
            } else {
                HashSet<PageId> pids = new HashSet<PageId> ();
                if(pids.add(pid)) {
                    wLockReverseIndex.put(tid, pids);
                }
            }
        }
        return true;
    }
}