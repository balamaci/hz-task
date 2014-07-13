package com.fortsoft.hztask.op.agent;


import com.hazelcast.core.Member;

/**
 * @author Serban Balamaci
 */
public class AnnounceMasterMemberOp extends AbstractAgentOp {

    private String masterUuid;

    public AnnounceMasterMemberOp(Member master) {
        this.masterUuid = master.getUuid();
    }

    @Override
    public Object call() throws Exception {
        System.out.println("Calling announce member " + masterUuid);
        try {
            getClusterAgentService().announceMaster(masterUuid);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }
}
