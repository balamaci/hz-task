package ro.fortsoft.hztask.op.agent;


import com.hazelcast.core.Member;

/**
 * @author Serban Balamaci
 */
public class AnnounceMasterAndSignalStartWorkOp extends AbstractAgentOp<Boolean> {

    private String masterUuid;

    public AnnounceMasterAndSignalStartWorkOp(Member master) {
        this.masterUuid = master.getUuid();
    }

    @Override
    public Boolean call() throws Exception {
        boolean masterChanged = getClusterAgentService().setMaster(masterUuid);
        if(masterChanged) {
            getClusterAgentService().startWork();
            return Boolean.TRUE;
        }
        return Boolean.FALSE;
    }
}
