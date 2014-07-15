package ro.fortsoft.hztask.op.agent;


import com.hazelcast.core.Member;

/**
 * @author Serban Balamaci
 */
public class AnnounceMasterAndSignalStartWorkOp extends AbstractAgentOp {

    private String masterUuid;

    public AnnounceMasterAndSignalStartWorkOp(Member master) {
        this.masterUuid = master.getUuid();
    }

    @Override
    public Object call() throws Exception {
        try {
            getClusterAgentService().setMaster(masterUuid);
            getClusterAgentService().startWork();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }
}
