package com.fortsoft.hztask.cluster;

/**
 * @author Serban Balamaci
 */
public interface IClusterAgentService {

    public boolean isActive();

    public void announceMaster(String masterUuid);

    public void shutdown();

}
