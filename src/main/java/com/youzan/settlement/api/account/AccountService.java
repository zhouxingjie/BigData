package com.youzan.settlement.api.account;

/**
 * Created by xingjie.zhou on 16/9/1.
 */
public interface AccountService {

    int getByAcctNo(String acctNo);

    String getAcctName(String acctNo);
}
