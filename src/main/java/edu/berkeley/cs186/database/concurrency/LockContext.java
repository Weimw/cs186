package edu.berkeley.cs186.database.concurrency;

import edu.berkeley.cs186.database.TransactionContext;
import edu.berkeley.cs186.database.common.Pair;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * LockContext wraps around LockManager to provide the hierarchical structure
 * of multigranularity locking. Calls to acquire/release/etc. locks should
 * be mostly done through a LockContext, which provides access to locking
 * methods at a certain point in the hierarchy (database, table X, etc.)
 */
public class LockContext {
    // You should not remove any of these fields. You may add additional fields/methods as you see fit.

    // The underlying lock manager.
    protected final LockManager lockman;
    // The parent LockContext object, or null if this LockContext is at the top of the hierarchy.
    protected final LockContext parent;
    // The name of the resource this LockContext represents.
    protected ResourceName name;
    // Whether this LockContext is readonly. If a LockContext is readonly, acquire/release/promote/escalate should
    // throw an UnsupportedOperationException.
    protected boolean readonly;
    // A mapping between transaction numbers, and the number of locks on children of this LockContext
    // that the transaction holds.
    protected final Map<Long, Integer> numChildLocks;
    // The number of children that this LockContext has, if it differs from the number of times
    // LockContext#childContext was called with unique parameters: for a table, we do not
    // explicitly create a LockContext for every page (we create them as needed), but
    // the capacity should be the number of pages in the table, so we use this
    // field to override the return value for capacity().
    protected int capacity;

    // You should not modify or use this directly.
    protected final Map<Long, LockContext> children;

    // Whether or not any new child LockContexts should be marked readonly.
    protected boolean childLocksDisabled;

    public LockContext(LockManager lockman, LockContext parent, Pair<String, Long> name) {
        this(lockman, parent, name, false);
    }

    protected LockContext(LockManager lockman, LockContext parent, Pair<String, Long> name,
                          boolean readonly) {
        this.lockman = lockman;
        this.parent = parent;
        if (parent == null) {
            this.name = new ResourceName(name);
        } else {
            this.name = new ResourceName(parent.getResourceName(), name);
        }
        this.readonly = readonly;
        this.numChildLocks = new ConcurrentHashMap<>();
        this.capacity = -1;
        this.children = new ConcurrentHashMap<>();
        this.childLocksDisabled = readonly;
    }

    /**
     * Gets a lock context corresponding to NAME from a lock manager.
     */
    public static LockContext fromResourceName(LockManager lockman, ResourceName name) {
        Iterator<Pair<String, Long>> names = name.getNames().iterator();
        LockContext ctx;
        Pair<String, Long> n1 = names.next();
        ctx = lockman.context(n1.getFirst(), n1.getSecond());
        while (names.hasNext()) {
            Pair<String, Long> p = names.next();
            ctx = ctx.childContext(p.getFirst(), p.getSecond());
        }
        return ctx;
    }

    /**
     * Get the name of the resource that this lock context pertains to.
     */
    public ResourceName getResourceName() {
        return name;
    }

    /**
     * Acquire a LOCKTYPE lock, for transaction TRANSACTION.
     *
     * Note: you *must* make any necessary updates to numChildLocks, or
     * else calls to LockContext#saturation will not work properly.
     *
     * @throws InvalidLockException if the request is invalid
     * @throws DuplicateLockRequestException if a lock is already held by TRANSACTION
     * @throws UnsupportedOperationException if context is readonly
     */
    public void acquire(TransactionContext transaction, LockType lockType)
    throws InvalidLockException, DuplicateLockRequestException {
        if (readonly && (lockType.equals(LockType.X) || lockType.equals(LockType.IX))) {
            throw new UnsupportedOperationException("Lock context is read only.");
        }
        if (hasSIXAncestor(transaction)
                && (lockType.equals(LockType.IS) || lockType.equals(LockType.S))) {
            throw new InvalidLockException("A SIX lock is held by ancestor. Any S or IS lock is redundant.");
        }

        if (this.parent != null) {
            LockType parentLockType = this.parent.getEffectiveLockType(transaction);
            if (!LockType.canBeParentLock(parentLockType, lockType)) {
                throw new InvalidLockException("Invalid lock type: Current context holding "
                        + parentLockType.toString() + " while requesting " + lockType.toString());
            }
        }

        lockman.acquire(transaction, name, lockType);
        addNumChildLocks(transaction, parent);
    }

    /**
     * Release TRANSACTION's lock on NAME.
     *
     * Note: you *must* make any necessary updates to numChildLocks, or
     * else calls to LockContext#saturation will not work properly.
     *
     * @throws NoLockHeldException if no lock on NAME is held by TRANSACTION
     * @throws InvalidLockException if the lock cannot be released (because doing so would
     *  violate multigranularity locking constraints)
     * @throws UnsupportedOperationException if context is readonly
     */
    public void release(TransactionContext transaction)
    throws NoLockHeldException, InvalidLockException {
        if (readonly) {
            throw new UnsupportedOperationException("Lock context is read only.");
        }
        if (getExplicitLockType(transaction).equals(LockType.NL)) {
            throw new NoLockHeldException("No lock is held on " + name.toString());
        }
        if (!numChildLocks.getOrDefault(transaction.getTransNum(), 0).equals(0)) {
            throw new InvalidLockException("Child context is still holding locks.");
        }
        lockman.release(transaction, this.name);
        delNumChildLocks(transaction, parent);
    }

    /**
     * Promote TRANSACTION's lock to NEWLOCKTYPE. For promotion to SIX from IS/IX/S, all S,
     * IS, and SIX locks on descendants must be simultaneously released.
     *
     * Note: you *must* make any necessary updates to numChildLocks, or
     * else calls to LockContext#saturation will not work properly.
     *
     * @throws DuplicateLockRequestException if TRANSACTION already has a NEWLOCKTYPE lock
     * @throws NoLockHeldException if TRANSACTION has no lock
     * @throws InvalidLockException if the requested lock type is not a promotion or promoting
     * would cause the lock manager to enter an invalid state (e.g. IS(parent), X(child)). A promotion
     * from lock type A to lock type B is valid if B is substitutable
     * for A and B is not equal to A, or if B is SIX and A is IS/IX/S, and invalid otherwise.
     * @throws UnsupportedOperationException if context is readonly
     */
    public void promote(TransactionContext transaction, LockType newLockType)
    throws DuplicateLockRequestException, NoLockHeldException, InvalidLockException {
        if (readonly) {
            throw new UnsupportedOperationException("Lock context is read only.");
        }
        LockType type = getExplicitLockType(transaction);
        if (type.equals(LockType.NL)) {
            throw new NoLockHeldException("No lock is held on " + name.toString());
        }
        if (type.equals(newLockType)) {
            throw new DuplicateLockRequestException("No need to promote the lock.");
        }
        if (!newLockType.equals(LockType.SIX)) {
            if (LockType.substitutable(newLockType, type)) {
                lockman.promote(transaction, name, newLockType);
            } else {
                throw new InvalidLockException("Invalid promotion from "
                        + type.toString() + " to " + newLockType.toString());
            }
        } else {
            List<ResourceName> toReleaseNames = sisDescendants(transaction);
            List<LockContext> contexts = new ArrayList<>();
            toReleaseNames.add(name);
            for (ResourceName resourceName : toReleaseNames) {
                contexts.add(fromResourceName(lockman, resourceName));
            }
            lockman.acquireAndRelease(transaction, name, newLockType, toReleaseNames);
            for (LockContext context : contexts) {
                context.delNumChildLocks(transaction, context.parent);
            }
        }
    }

    /**
     * Escalate TRANSACTION's lock from descendants of this context to this level, using either
     * an S or X lock. There should be no descendant locks after this
     * call, and every operation valid on descendants of this context before this call
     * must still be valid. You should only make *one* mutating call to the lock manager,
     * and should only request information about TRANSACTION from the lock manager.
     *
     * For example, if a transaction has the following locks:
     *      IX(database) IX(table1) S(table2) S(table1 page3) X(table1 page5)
     * then after table1Context.escalate(transaction) is called, we should have:
     *      IX(database) X(table1) S(table2)
     *
     * You should not make any mutating calls if the locks held by the transaction do not change
     * (such as when you call escalate multiple times in a row).
     *
     * Note: you *must* make any necessary updates to numChildLocks of all relevant contexts, or
     * else calls to LockContext#saturation will not work properly.
     *
     * @throws NoLockHeldException if TRANSACTION has no lock at this level
     * @throws UnsupportedOperationException if context is readonly
     */
    public void escalate(TransactionContext transaction) throws NoLockHeldException {
        if (readonly) {
            throw new UnsupportedOperationException("Lock context is read only.");
        }
        LockType type = getExplicitLockType(transaction);
        if (type.equals(LockType.NL)) {
            throw new NoLockHeldException("No lock is held on " + name.toString());
        }
        if (type.equals(LockType.S) || type.equals(LockType.X)) return;

        List<ResourceName> toReleaseNames = allDescendants(transaction);
        toReleaseNames.add(name);
        List<LockContext> contexts = new ArrayList<>();
        for (ResourceName resourceName : toReleaseNames) {
            LockContext context = fromResourceName(lockman, resourceName);
            contexts.add(context);
        }
        if (type.equals(LockType.IS)) {
            lockman.acquireAndRelease(transaction, name, LockType.S, toReleaseNames);
            for (LockContext context: contexts) {
                context.delNumChildLocks(transaction, context.parent);
            }
        } else if (type.equals(LockType.IX) || type.equals(LockType.SIX)) {
            lockman.acquireAndRelease(transaction, name, LockType.X, toReleaseNames);
            for (LockContext context: contexts) {
                context.delNumChildLocks(transaction, context.parent);
            }
        }
    }

    /**
     * Gets the type of lock that the transaction has at this level, either implicitly
     * (e.g. explicit S lock at higher level implies S lock at this level) or explicitly.
     * Returns NL if there is no explicit nor implicit lock.
     */
    public LockType getEffectiveLockType(TransactionContext transaction) {
        if (transaction == null) {
            return LockType.NL;
        }
        LockType explicitLockType = this.getExplicitLockType(transaction);
        if (!explicitLockType.equals(LockType.NL)) return explicitLockType;
        LockContext pointer = this.parent;
        while (pointer != null) {
            if (!pointer.getExplicitLockType(transaction).equals(LockType.NL)) {
                break;
            }
            pointer = pointer.parent;
        }
        if (pointer == null) return LockType.NL;
        LockType ancestorLockType = pointer.getExplicitLockType(transaction);
        if (ancestorLockType.equals(LockType.SIX)) return LockType.S;
        if (ancestorLockType.equals(LockType.IS)
                || ancestorLockType.equals(LockType.IX)) return LockType.NL;
        return ancestorLockType;

    }

    /**
     * Get the type of lock that TRANSACTION holds at this level, or NL if no lock is held at this level.
     */
    public LockType getExplicitLockType(TransactionContext transaction) {
        if (transaction == null) {
            return LockType.NL;
        }
        return lockman.getLockType(transaction, getResourceName());
    }

    /**
     * Helper method to see if the transaction holds a SIX lock at an ancestor of this context
     * @param transaction the transaction
     * @return true if holds a SIX at an ancestor, false if not
     */
    private boolean hasSIXAncestor(TransactionContext transaction) {
        LockContext context = parent;
        while (context != null) {
            LockType parentLockType = lockman.getLockType(
                    transaction, context.getResourceName());
            if (parentLockType.equals(LockType.SIX)) { return true; }
            context = context.parent;
        }
        return false;
    }

    /**
     * Helper method to get a list of resourceNames of all locks that are S or IS and are descendants of current context
     * for the given transaction.
     * @param transaction the given transaction
     * @return a list of ResourceNames of descendants which the transaction holds a S or IS lock.
     */
    private List<ResourceName> sisDescendants(TransactionContext transaction) {
        List<Lock> locks = lockman.getLocks(transaction);
        List<ResourceName> names = new ArrayList<>();
        for (Lock lock : locks) {
            if (lock.lockType.equals(LockType.S) || lock.lockType.equals(LockType.IS)) {
                LockContext context = fromResourceName(lockman, lock.name);
                if (isAncestor(context, this)) {
                    names.add(lock.name);
                }
            }
        }
        return names;
    }

    /**
     * Helper method to get a list of resourceNames of all locks are descendants of current context
     * for the given transaction.
     * @param transaction the given transaction
     * @return a list of ResourceNames of descendants.
     */
    private List<ResourceName> allDescendants(TransactionContext transaction) {
        List<Lock> locks = lockman.getLocks(transaction);
        List<ResourceName> names = new ArrayList<>();
        for (Lock lock : locks) {
            LockContext context = fromResourceName(lockman, lock.name);
            if (isAncestor(context, this)) {
                names.add(lock.name);
            }
        }
        return names;
    }

    /**
     * Helper method to increment count of numChildLocks for given transaction.
     * This method propagates above until it reaches the highest level.
     * @param transaction the given transaction
     * @param context the context to be modified
     */
    private void addNumChildLocks(TransactionContext transaction, LockContext context) {
        if (context == null) return;
        Long transactionNum = transaction.getTransNum();
        int numLocks = context.numChildLocks.getOrDefault(transactionNum, 0);
        context.numChildLocks.put(transactionNum, numLocks + 1);
        addNumChildLocks(transaction, context.parent);
    }

    /**
     * Helper method to decrement count of numChildLocks for given transaction.
     * This method propagates above until it reaches the highest level.
     * @param transaction the given transaction
     * @param context the context to be modified
     */
    private void delNumChildLocks(TransactionContext transaction, LockContext context) {
        if (context == null) return;
        Long transactionNum = transaction.getTransNum();
        int numLocks = context.numChildLocks.getOrDefault(transactionNum, 0);
        if (numLocks == 0) return;
        context.numChildLocks.put(transactionNum, numLocks - 1);
        delNumChildLocks(transaction, context.parent);
    }

    private boolean isAncestor(LockContext child, LockContext ancestor) {
        if (child == null) return false;
        if (ancestor == null) return true;
        LockContext pointer = child.parent;
        while (pointer != null) {
            if (pointer.equals(ancestor)) return true;
            pointer = pointer.parent;
        }
        return false;
    }


    /**
     * Disables locking descendants. This causes all new child contexts of this context
     * to be readonly. This is used for indices and temporary tables (where
     * we disallow finer-grain locks), the former due to complexity locking
     * B+ trees, and the latter due to the fact that temporary tables are only
     * accessible to one transaction, so finer-grain locks make no sense.
     */
    public void disableChildLocks() {
        this.childLocksDisabled = true;
    }

    /**
     * Gets the parent context.
     */
    public LockContext parentContext() {
        return parent;
    }

    /**
     * Gets the context for the child with name NAME (with a readable version READABLE).
     */
    public synchronized LockContext childContext(String readable, long name) {
        LockContext temp = new LockContext(lockman, this, new Pair<>(readable, name),
                                           this.childLocksDisabled || this.readonly);
        LockContext child = this.children.putIfAbsent(name, temp);
        if (child == null) {
            child = temp;
        }
        if (child.name.getCurrentName().getFirst() == null && readable != null) {
            child.name = new ResourceName(this.name, new Pair<>(readable, name));
        }
        return child;
    }

    /**
     * Gets the context for the child with name NAME.
     */
    public synchronized LockContext childContext(long name) {
        return childContext(Long.toString(name), name);
    }

    /**
     * Sets the capacity (number of children).
     */
    public synchronized void capacity(int capacity) {
        this.capacity = capacity;
    }

    /**
     * Gets the capacity. Defaults to number of child contexts if never explicitly set.
     */
    public synchronized int capacity() {
        return this.capacity < 0 ? this.children.size() : this.capacity;
    }

    /**
     * Gets the saturation (number of locks held on children / number of children) for
     * a single transaction. Saturation is 0 if number of children is 0.
     */
    public double saturation(TransactionContext transaction) {
        if (transaction == null || capacity() == 0) {
            return 0.0;
        }
        return ((double) numChildLocks.getOrDefault(transaction.getTransNum(), 0)) / capacity();
    }

    @Override
    public String toString() {
        return "LockContext(" + name.toString() + ")";
    }
}

