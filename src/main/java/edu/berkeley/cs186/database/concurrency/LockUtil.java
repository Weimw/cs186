package edu.berkeley.cs186.database.concurrency;

import edu.berkeley.cs186.database.TransactionContext;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * LockUtil is a declarative layer which simplifies multigranularity lock acquisition
 * for the user (you, in the second half of Part 2). Generally speaking, you should use LockUtil
 * for lock acquisition instead of calling LockContext methods directly.
 */
public class LockUtil {
    /**
     * Ensure that the current transaction can perform actions requiring LOCKTYPE on LOCKCONTEXT.
     *
     * This method should promote/escalate as needed, but should only grant the least
     * permissive set of locks needed.
     *
     * lockType is guaranteed to be one of: S, X, NL.
     *
     * If the current transaction is null (i.e. there is no current transaction), this method should do nothing.
     */
    public static void ensureSufficientLockHeld(LockContext lockContext, LockType lockType) {
        TransactionContext transaction = TransactionContext.getTransaction(); // current transaction
        if (transaction == null || lockType.equals(LockType.NL)) return;
        LockType effectiveLockType = lockContext.getEffectiveLockType(transaction);
        if (LockType.substitutable(effectiveLockType, lockType)) return;

        if (lockType.equals(LockType.S)) {
            List<LockContext> ancestors = getAncestors(lockContext);
            for (LockContext ancestor : ancestors) {
                LockType ancestorLockType = ancestor.getExplicitLockType(transaction);
                if (ancestorLockType.equals(LockType.NL)) {
                    ancestor.acquire(transaction, LockType.IS);
                }
            }

            LockType explicitLockType = lockContext.getExplicitLockType(transaction);
            if (explicitLockType.equals(LockType.NL)) {
                lockContext.acquire(transaction, LockType.S);
            } else if (explicitLockType.equals(LockType.IS)) {
                lockContext.escalate(transaction);
            } else {
                lockContext.promote(transaction, LockType.SIX);
            }
        } else {
            List<LockContext> ancestors = getAncestors(lockContext);
            for (LockContext ancestor : ancestors) {
                LockType ancestorLockType = ancestor.getExplicitLockType(transaction);
                if (ancestorLockType.equals(LockType.NL)) {
                    ancestor.acquire(transaction, LockType.IX);
                } else if (ancestorLockType.equals(LockType.IS)) {
                    ancestor.promote(transaction, LockType.IX);
                } else if (ancestorLockType.equals(LockType.S)) {
                    ancestor.promote(transaction, LockType.SIX);
                }
            }

            LockType explicitLockType = lockContext.getExplicitLockType(transaction);
            if (explicitLockType.equals(LockType.NL)) {
                lockContext.acquire(transaction, LockType.X);
            } else if (explicitLockType.equals(LockType.IS)) {
                lockContext.escalate(transaction);
                lockContext.promote(transaction, LockType.X);
            }else if (explicitLockType.equals(LockType.S)) {
                lockContext.promote(transaction, LockType.X);
            } else {
                lockContext.escalate(transaction);
            }
        }
    }

    /**
     * Helper method to get all ancestors. Return the ancestors in a top-down manner.
     */
    private static List<LockContext> getAncestors(LockContext context) {
        List<LockContext> ancestors = new ArrayList<>();
        LockContext pointer = context.parentContext();
        while (pointer != null) {
            ancestors.add(pointer);
            pointer = pointer.parentContext();
        }
        Collections.reverse(ancestors);
        return ancestors;
    }
}
