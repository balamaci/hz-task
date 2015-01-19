package ro.fortsoft.hztask.master.router;

import com.google.common.base.Optional;
import com.hazelcast.core.Member;
import ro.fortsoft.hztask.common.task.Task;

/**
 * Generic interface for implementing different strategies of assigning
 * next task to a cluster member
 *
 * @author Serban Balamaci
 */
public interface RoutingStrategy {

    public enum Type  {
        ROUND_ROBIN(),
        BALANCED_LOAD_ORIENTED(),
        BALANCED_LOW_FAILURE_ORIENTED();
    }

    /**
     * Picks a cluster member on which to run the task
     * @param task task
     * @return cluster member
     */
    public Optional<Member> getMemberToRunOn(Task task);

}
