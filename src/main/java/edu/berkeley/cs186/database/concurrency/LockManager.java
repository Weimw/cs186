package edu.berkeley.cs186.database.concurrency;

import edu.berkeley.cs186.database.TransactionContext;
import edu.berkeley.cs186.database.common.Pair;

import java.util.*;

/**
 * LockManager maintains the bookkeeping for what transactions have
 * what locks on what resources. The lock manager should generally **not**
 * be used directly: instead, code should call methods of LockContext to
 * acquire/release/promote/escalate locks.
 *
 * The LockManager is primarily concerned with the mappings between
 * transactions, resources, and locks, and does not concern itself with
 * multiple levels of granularity (you can and should treat ResourceName
 * as a generic Object, rather than as an object encapsulating levels of
 * granularity, in this class).
 *
 * It follows that LockManager should allow **all**
 * requests that are valid from the perspective of treating every resource
 * as independent objects, even if they would be invalid from a
 * multigranularity locking perspective. For example, if LockManager#acquire
 * is called asking for an X lock on Table A, and the transaction has no
 * locks at the time, the request is considered valid (because the only problem
 * with such a request would be that the transaction does not have the appropriate
 * intent locks, but that is a multigranularity concern).
 *
 * Each resource the lock manager manages has its own queue of LockRequest objects
 * representing a request to acquire (or promote/acquire-and-release) a lock that
 * could not be satisfied at the time. This queue should be processed every time
 * a lock on that resource gets released, starting from the first request, and going
 * in order until a request cannot be satisfied. Requests taken off the queue should
 * be treated as if that transaction had made the request right after the resource was
 * released in absence of a queue (i.e. removing a request by T1 to acquire X(db) should
 * be treated as if T1 had just requested X(db) and there were no queue on db: T1 should
 * be given the X lock on db, and put in an unblocked state via Transaction#unblock).
 *
 * This does mean that in the case of:
 *    queue: S(A) X(A) S(A)
 * only the first request should be removed from the queue when the queue is processed.
 */
public class LockManager {
    // transactionLocks is a mapping from transaction number to a list of lock
    // objects held by that transaction.
    Map<Long, List<Lock>> transactionLocks = new HashMap<>();
    // resourceEntries is a mapping from resource names to a ResourceEntry
    // object, which contains a list of Locks on the object, as well as a
    // queue for requests on that resource.
    Map<ResourceName, ResourceEntry> resourceEntries = new HashMap<>();

    // A ResourceEntry contains the list of locks on a resource, as well as
    // the queue for requests for locks on the resource.
    private class ResourceEntry {
        // List of currently granted locks on the resource.
        private List<Lock> locks = new ArrayList<>();
        // Queue for yet-to-be-satisfied lock requests on this resource.
        private Deque<LockRequest> waitingQueue = new ArrayDeque<>();

        /* Helper function to check if a resource entry is locked. */
        public boolean isLocked() {
            return locks.size() > 0;
        }

        /* Helper function to check if a request is waiting on a resource. */
        public boolean isWaiting() {
            return waitingQueue.size() > 0;
        }

        /* Helper function to check if a given lock is compatible with the locks on the entry. */
        public boolean isCompatible(Lock givenLock) {
            for (Lock lock: locks) {
                if (lock.transactionNum != givenLock.transactionNum
                        && !LockType.compatible(givenLock.lockType, lock.lockType)) {
                    return false;
                }
            }
            return true;
        }

        /* Helper function to add one lock to the entry. Check compatibility before calling this. */
        public void addLock(Lock lock) {
            assert !locks.contains(lock);
            locks.add(lock);
        }

        /* Helper function to remove one lock to the entry. */
        public void removeLock(Lock lock) {
            assert locks.contains(lock);
            locks.remove(lock);
        }

        /* Helper function to add one request to the back of the deque. */
        public void addWaiting(LockRequest request) {
            assert !waitingQueue.contains(request);
            waitingQueue.add(request);
        }

        /* Helper function to add one request to the head of the deque. */
        public void addFirstWaiting(LockRequest request) {
            assert !waitingQueue.contains(request);
            waitingQueue.addFirst(request);
        }

        /* Helper function to get the head of the waiting deque. */
        public LockRequest getFirstWaiting() {
            assert !waitingQueue.isEmpty();
            return waitingQueue.getFirst();
        }

        /* Helper function to remove one request from the deque. */
        public void removeWaiting(LockRequest request) {
            assert waitingQueue.contains(request);
            waitingQueue.remove(request);
        }


        @Override
        public String toString() {
            return "Active Locks: " + Arrays.toString(this.locks.toArray()) +
                   ", Queue: " + Arrays.toString(this.waitingQueue.toArray());
        }
    }

    // You should not modify or use this directly.
    private Map<Long, LockContext> contexts = new HashMap<>();

    /**
     * Helper method to fetch the resourceEntry corresponding to NAME.
     * Inserts a new (empty) resourceEntry into the map if no entry exists yet.
     */
    private ResourceEntry getResourceEntry(ResourceName name) {
        resourceEntries.putIfAbsent(name, new ResourceEntry());
        return resourceEntries.get(name);
    }

    /**
     * Helper method to add a lock to Transaction determined by transactionNum.
     * Inserts a new List<Lock> into the map if no list for transaction exists yet.
     */
    private void addLockToTransaction(Long transactionNum, Lock lock) {
        if (transactionLocks.containsKey(transactionNum)) {
            List<Lock> locks = transactionLocks.get(transactionNum);
            locks.add(lock);
        } else {
            List<Lock> locks = new ArrayList<>(Arrays.asList(lock));
            transactionLocks.put(transactionNum, locks);
        }
    }

    /**
     * Helper method to remove a lock from Transaction determined by transactionNum.
     */
    private void removeLockFromTransaction(Long transactionNum, Lock lock) {
        assert transactionLocks.containsKey(transactionNum);
        List<Lock> locks = transactionLocks.get(transactionNum);
        assert locks.contains(lock);
        locks.remove(lock);
    }

    /**
     * Helper method to determine whether request can be unblocked.
     */

    private boolean isValid(LockRequest request, ResourceName name) {
        ResourceEntry entry = getResourceEntry(name);
        return entry.isCompatible(request.lock);
    }

    /**
     * Acquire a LOCKTYPE lock on NAME, for transaction TRANSACTION, and releases all locks
     * in RELEASELOCKS after acquiring the lock, in one atomic action.
     *
     * Error checking must be done before any locks are acquired or released. If the new lock
     * is not compatible with another transaction's lock on the resource, the transaction is
     * blocked and the request is placed at the **front** of ITEM's queue.
     *
     * Locks in RELEASELOCKS should be released only after the requested lock has been acquired.
     * The corresponding queues should be processed.
     *
     * An acquire-and-release that releases an old lock on NAME **should not** change the
     * acquisition time of the lock on NAME, i.e.
     * if a transaction acquired locks in the order: S(A), X(B), acquire X(A) and release S(A), the
     * lock on A is considered to have been acquired before the lock on B.
     *
     * @throws DuplicateLockRequestException if a lock on NAME is held by TRANSACTION and
     * isn't being released
     * @throws NoLockHeldException if no lock on a name in RELEASELOCKS is held by TRANSACTION
     */
    public void acquireAndRelease(TransactionContext transaction, ResourceName name,
                                  LockType lockType, List<ResourceName> releaseLocks)
    throws DuplicateLockRequestException, NoLockHeldException {
        if (!(this.getLockType(transaction, name).equals(LockType.NL) || releaseLocks.contains(name))) {
            throw new DuplicateLockRequestException("Duplicate request on resource " + name.toString());
        }
        for (ResourceName resourceName : releaseLocks) {
            if (this.getLockType(transaction, resourceName).equals(LockType.NL)) {
                throw new NoLockHeldException("No lock is held on resource " + name.toString());
            }
        }
        boolean blocked = false;
        // You may modify any part of this method. You are not required to keep all your
        // code within the given synchronized block -- in fact,
        // you will have to write some code outside the synchronized block to avoid locking up
        // the entire lock manager when a transaction is blocked. You are also allowed to
        // move the synchronized block elsewhere if you wish.
        synchronized (this) {
            ResourceEntry entry = getResourceEntry(name);
            Lock lock = new Lock(name, lockType, transaction.getTransNum());
            List<Lock> releasedLocks = new ArrayList<>(),
                    locksFromTransaction = transactionLocks.get(transaction.getTransNum());

            for (ResourceName resourceName : releaseLocks) {
                ResourceEntry entryToRelease = getResourceEntry(resourceName);
                for (Lock lockFromTransaction : locksFromTransaction) {
                    if (entryToRelease.locks.contains(lockFromTransaction)) {
                        releasedLocks.add(lockFromTransaction);
                    }
                }
            }

            if (!entry.isLocked() || (entry.isCompatible(lock) && !entry.isWaiting())) {
                entry.addLock(lock);
                addLockToTransaction(transaction.getTransNum(), lock);

                for (Lock lockToRelease : releasedLocks) {
                    release(transaction, lockToRelease.name);
                }
            } else {
                entry.addFirstWaiting(new LockRequest(transaction, lock, releasedLocks));
                transaction.prepareBlock();
                blocked = true;
            }
        }
        if (blocked) {
            transaction.block();
        }
    }

    /**
     * Acquire a LOCKTYPE lock on NAME, for transaction TRANSACTION.
     *
     * Error checking must be done before the lock is acquired. If the new lock
     * is not compatible with another transaction's lock on the resource, or if there are
     * other transaction in queue for the resource, the transaction is
     * blocked and the request is placed at the **back** of NAME's queue.
     *
     * @throws DuplicateLockRequestException if a lock on NAME is held by
     * TRANSACTION
     */
    public void acquire(TransactionContext transaction, ResourceName name,
                        LockType lockType) throws DuplicateLockRequestException {
        if (!this.getLockType(transaction, name).equals(LockType.NL)) {
            throw new DuplicateLockRequestException("Duplicate request on resource " + name.toString());
        }
        boolean blocked = false;
        // You may modify any part of this method. You are not required to keep all your
        // code within the given synchronized block -- in fact,
        // you will have to write some code outside the synchronized block to avoid locking up
        // the entire lock manager when a transaction is blocked. You are also allowed to
        // move the synchronized block elsewhere if you wish.
        synchronized (this) {
            ResourceEntry entry = getResourceEntry(name);
            Lock lock = new Lock(name, lockType, transaction.getTransNum());
            if (!entry.isLocked() || (entry.isCompatible(lock) && !entry.isWaiting())) {
                entry.addLock(lock);
                addLockToTransaction(transaction.getTransNum(), lock);
            } else {
                entry.addWaiting(new LockRequest(transaction, lock));
                transaction.prepareBlock();
                blocked = true;
            }
        }
        if (blocked) {
            transaction.block();
        }
    }

    /**
     * Release TRANSACTION's lock on NAME.
     *
     * Error checking must be done before the lock is released.
     *
     * NAME's queue should be processed after this call. If any requests in
     * the queue have locks to be released, those should be released, and the
     * corresponding queues also processed.
     *
     * @throws NoLockHeldException if no lock on NAME is held by TRANSACTION
     */
    public void release(TransactionContext transaction, ResourceName name)
    throws NoLockHeldException {
        LockType type = this.getLockType(transaction, name);
        if (type.equals(LockType.NL)) {
            throw new NoLockHeldException("No lock is held on resource " + name.toString());
        }
        synchronized (this) {
            ResourceEntry entry = getResourceEntry(name);
            Lock lock = new Lock(name, type, transaction.getTransNum());
            removeLockFromTransaction(transaction.getTransNum(), lock);
            entry.removeLock(lock);


            /*
            if (entry.isWaiting()) {
                LockRequest request = entry.getFirstWaiting();
                if (isValid(request, name)) {
                    entry.removeWaiting(request);

                    acquire(request.transaction, request.lock.name, request.lock.lockType);
                    for (Lock lockToRelease : request.releasedLocks) {
                        release(request.transaction, lockToRelease.name);
                    }

                    request.transaction.unblock();
                }
            }
            */

        }
    }

    /**
     * Promote TRANSACTION's lock on NAME to NEWLOCKTYPE (i.e. change TRANSACTION's lock
     * on NAME from the current lock type to NEWLOCKTYPE, which must be strictly more
     * permissive).
     *
     * Error checking must be done before any locks are changed. If the new lock
     * is not compatible with another transaction's lock on the resource, the transaction is
     * blocked and the request is placed at the **front** of ITEM's queue.
     *
     * A lock promotion **should not** change the acquisition time of the lock, i.e.
     * if a transaction acquired locks in the order: S(A), X(B), promote X(A), the
     * lock on A is considered to have been acquired before the lock on B.
     *
     * @throws DuplicateLockRequestException if TRANSACTION already has a
     * NEWLOCKTYPE lock on NAME
     * @throws NoLockHeldException if TRANSACTION has no lock on NAME
     * @throws InvalidLockException if the requested lock type is not a promotion. A promotion
     * from lock type A to lock type B is valid if and only if B is substitutable
     * for A, and B is not equal to A.
     */
    public void promote(TransactionContext transaction, ResourceName name,
                        LockType newLockType)
    throws DuplicateLockRequestException, NoLockHeldException, InvalidLockException {
        if (!this.getLockType(transaction, name).equals(newLockType)) {
            throw new DuplicateLockRequestException("Duplicate request on resource " + name.toString());
        }
        if (this.getLockType(transaction, name).equals(LockType.NL)) {
            throw new NoLockHeldException("No lock is held on resource " + name.toString());
        }
        if (!LockType.substitutable(newLockType, this.getLockType(transaction, name))) {
            throw new InvalidLockException("Not a promotion.");
        }
        // You may modify any part of this method.
        synchronized (this) {
            ResourceEntry entry = getResourceEntry(name);
            Lock promotedLock = new Lock(name, newLockType, transaction.getTransNum());
            if (entry.isCompatible(promotedLock)) {
                release(transaction, name);
                acquire(transaction, name, newLockType);
            } else {
                entry.addFirstWaiting(new LockRequest(transaction, promotedLock));
            }
        }
    }

    /**
     * Return the type of lock TRANSACTION has on NAME (return NL if no lock is held).
     */
    public synchronized LockType getLockType(TransactionContext transaction, ResourceName name) {
        List<Lock> locksOnName = getLocks(name);
        List<Lock> locksOnTransaction = getLocks(transaction);
        for (Lock lock : locksOnName) {
            if (locksOnTransaction.contains(lock)) {
                return lock.lockType;
            }
        }
        return LockType.NL;
    }

    /**
     * Returns the list of locks held on NAME, in order of acquisition.
     * A promotion or acquire-and-release should count as acquired
     * at the original time.
     */
    public synchronized List<Lock> getLocks(ResourceName name) {
        return new ArrayList<>(resourceEntries.getOrDefault(name, new ResourceEntry()).locks);
    }

    /**
     * Returns the list of locks locks held by
     * TRANSACTION, in order of acquisition. A promotion or
     * acquire-and-release should count as acquired at the original time.
     */
    public synchronized List<Lock> getLocks(TransactionContext transaction) {
        return new ArrayList<>(transactionLocks.getOrDefault(transaction.getTransNum(),
                               Collections.emptyList()));
    }

    /**
     * Creates a lock context. See comments at
     * he top of this file and the top of LockContext.java for more information.
     */
    public synchronized LockContext context(String readable, long name) {
        if (!contexts.containsKey(name)) {
            contexts.put(name, new LockContext(this, null, new Pair<>(readable, name)));
        }
        return contexts.get(name);
    }

    /**
     * Create a lock context for the database. See comments at
     * the top of this file and the top of LockContext.java for more information.
     */
    public synchronized LockContext databaseContext() {
        return context("database", 0L);
    }
}
