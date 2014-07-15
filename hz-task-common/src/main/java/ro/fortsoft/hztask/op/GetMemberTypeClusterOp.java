package ro.fortsoft.hztask.op;

import ro.fortsoft.hztask.common.HzKeysConstants;
import ro.fortsoft.hztask.common.MemberType;

/**
 * @author Serban Balamaci
 */
public class GetMemberTypeClusterOp extends AbstractClusterOp<MemberType> {

    @Override
    public MemberType call() throws Exception {
        return (MemberType) getHzInstance().getUserContext().
                get(HzKeysConstants.USER_CONTEXT_MEMBER_TYPE);
    }
}
