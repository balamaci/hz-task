package ro.fortsoft.hztask.cluster;

/**
 * Interface through which the Master by sending AbstractAgentOps to the ClusterAgent gets a reference
 * to the clusterMasterService which is exposed on the ClusterMaster and can call the methods exposed
 * by this interface.
 *
 * @author Serban Balamaci
 */
public interface IClusterAgentService {

    public boolean isActive();

    public boolean setMaster(String masterUuid);

    public void startWork();

    public void shutdown();

}
