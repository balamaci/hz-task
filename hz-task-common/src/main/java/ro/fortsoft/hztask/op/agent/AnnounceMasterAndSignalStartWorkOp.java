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
            boolean masterChanged = getClusterAgentService().setMaster(masterUuid);
            if(masterChanged) {
                getClusterAgentService().startWork();
                return Boolean.TRUE;
            }
            return Boolean.FALSE;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }
}
