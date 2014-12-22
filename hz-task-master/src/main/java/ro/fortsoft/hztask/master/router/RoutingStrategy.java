package ro.fortsoft.hztask.master.router;

import com.google.common.base.Optional;
import com.hazelcast.core.Member;
import ro.fortsoft.hztask.common.task.Task;

/**
 * @author Serban Balamaci
 */
public interface RoutingStrategy {

    public Optional<Member> getMemberToRunOn(Task task);

}
