package ro.fortsoft.hztask.op;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;

import java.io.Serializable;
import java.util.concurrent.Callable;

/**
 * @author Serban Balamaci
 */
public abstract class AbstractClusterOp<T> implements Serializable, HazelcastInstanceAware, Callable<T> {

    private transient HazelcastInstance hzInstance;

    @Override
    public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
        this.hzInstance = hazelcastInstance;
    }

    public HazelcastInstance getHzInstance() {
        return hzInstance;
    }

}
