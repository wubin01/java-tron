package org.tron.core.actuator;

import static junit.framework.TestCase.fail;
import static org.tron.protos.contract.Common.ResourceCode.BANDWIDTH;
import static org.tron.protos.contract.Common.ResourceCode.ENERGY;

import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import java.io.File;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.tron.common.application.TronApplicationContext;
import org.tron.common.utils.ByteArray;
import org.tron.common.utils.FileUtil;
import org.tron.core.Constant;
import org.tron.core.Wallet;
import org.tron.core.capsule.AccountCapsule;
import org.tron.core.capsule.TransactionResultCapsule;
import org.tron.core.capsule.WitnessCapsule;
import org.tron.core.config.DefaultConfig;
import org.tron.core.config.args.Args;
import org.tron.core.db.Manager;
import org.tron.core.exception.BalanceInsufficientException;
import org.tron.core.exception.ContractExeException;
import org.tron.core.exception.ContractValidateException;
import org.tron.protos.Protocol.Account.UnFreezeV2;
import org.tron.protos.Protocol.AccountType;
import org.tron.protos.Protocol.Transaction.Result.code;
import org.tron.protos.contract.AssetIssueContractOuterClass;
import org.tron.protos.contract.BalanceContract.WithdrawExpireUnfreezeContract;

@Slf4j
public class WithdrawExpireUnfreezeActuatorTest {

  private static final String dbPath = "output_withdraw_expire_unfreeze_test";
  private static final String OWNER_ADDRESS;
  private static final String OWNER_ADDRESS_INVALID = "aaaa";
  private static final String OWNER_ACCOUNT_INVALID;
  private static final long initBalance = 10_000_000_000L;
  private static final long allowance = 32_000_000L;
  private static Manager dbManager;
  private static TronApplicationContext context;

  static {
    Args.setParam(new String[]{"--output-directory", dbPath}, Constant.TEST_CONF);
    context = new TronApplicationContext(DefaultConfig.class);
    OWNER_ADDRESS = Wallet.getAddressPreFixString() + "548794500882809695a8a687866e76d4271a1abc";
    OWNER_ACCOUNT_INVALID =
        Wallet.getAddressPreFixString() + "548794500882809695a8a687866e76d4271a3456";
  }

  /**
   * Init data.
   */
  @BeforeClass
  public static void init() {
    dbManager = context.getBean(Manager.class);
    dbManager.getDynamicPropertiesStore().saveUnfreezeDelayDays(1L);
    dbManager.getDynamicPropertiesStore().saveAllowNewResourceModel(1L);
  }

  /**
   * Release resources.
   */
  @AfterClass
  public static void destroy() {
    Args.clearParam();
    context.destroy();
    if (FileUtil.deleteDir(new File(dbPath))) {
      logger.info("Release resources successful.");
    } else {
      logger.info("Release resources failure.");
    }
  }

  /**
   * create temp Capsule test need.
   */
  @Before
  public void createAccountCapsule() {
    AccountCapsule ownerCapsule = new AccountCapsule(ByteString.copyFromUtf8("owner"),
        ByteString.copyFrom(ByteArray.fromHexString(OWNER_ADDRESS)), AccountType.Normal,
        initBalance);
    UnFreezeV2 unFreezeV2_1 = UnFreezeV2.newBuilder().setType(BANDWIDTH)
        .setUnfreezeAmount(16_000_000L).setUnfreezeExpireTime(1).build();
    UnFreezeV2 unFreezeV2_2 = UnFreezeV2.newBuilder().setType(ENERGY)
        .setUnfreezeAmount(16_000_000L).setUnfreezeExpireTime(1).build();
    UnFreezeV2 unFreezeV2_3 = UnFreezeV2.newBuilder().setType(ENERGY)
        .setUnfreezeAmount(0).setUnfreezeExpireTime(Long.MAX_VALUE).build();
    ownerCapsule.addUnfrozenV2(unFreezeV2_1);
    ownerCapsule.addUnfrozenV2(unFreezeV2_2);
    ownerCapsule.addUnfrozenV2(unFreezeV2_3);
    dbManager.getAccountStore().put(ownerCapsule.createDbKey(), ownerCapsule);
  }

  private Any getContract(String ownerAddress) {
    return Any.pack(WithdrawExpireUnfreezeContract.newBuilder()
        .setOwnerAddress(ByteString.copyFrom(ByteArray.fromHexString(ownerAddress))).build());
  }

  @Test
  public void testWithdrawExpireUnfreeze() {
    long now = System.currentTimeMillis();
    dbManager.getDynamicPropertiesStore().saveLatestBlockHeaderTimestamp(now);
    byte[] address = ByteArray.fromHexString(OWNER_ADDRESS);

    AccountCapsule accountCapsule = dbManager.getAccountStore().get(address);
    Assert.assertEquals(accountCapsule.getLatestWithdrawTime(), 0);

    WitnessCapsule witnessCapsule = new WitnessCapsule(ByteString.copyFrom(address), 100,
        "http://baidu.com");
    dbManager.getWitnessStore().put(address, witnessCapsule);

    WithdrawExpireUnfreezeActuator actuator = new WithdrawExpireUnfreezeActuator();
    actuator.setChainBaseManager(dbManager.getChainBaseManager())
        .setAny(getContract(OWNER_ADDRESS));

    TransactionResultCapsule ret = new TransactionResultCapsule();

    try {
      actuator.validate();
      actuator.execute(ret);
      Assert.assertEquals(code.SUCESS, ret.getInstance().getRet());
      AccountCapsule owner = dbManager.getAccountStore()
          .get(ByteArray.fromHexString(OWNER_ADDRESS));
      List<UnFreezeV2> unfrozenV2List = owner.getInstance().getUnfrozenV2List();
      Assert.assertEquals(1, unfrozenV2List.size());
      Assert.assertEquals(Long.MAX_VALUE, unfrozenV2List.get(0).getUnfreezeExpireTime());
      Assert.assertEquals(initBalance + 32_000_000L, owner.getBalance());
      Assert.assertEquals(32_000_000L, ret.getWithdrawExpireAmount());
    } catch (ContractValidateException e) {
      Assert.assertFalse(e instanceof ContractValidateException);
    } catch (ContractExeException e) {
      Assert.assertFalse(e instanceof ContractExeException);
    }
  }


  @Test
  public void invalidOwnerAddress() {
    WithdrawExpireUnfreezeActuator actuator = new WithdrawExpireUnfreezeActuator();
    actuator.setChainBaseManager(dbManager.getChainBaseManager())
        .setAny(getContract(OWNER_ADDRESS_INVALID));

    TransactionResultCapsule ret = new TransactionResultCapsule();

    try {
      actuator.validate();
      actuator.execute(ret);
      fail("cannot run here.");

    } catch (ContractValidateException e) {
      Assert.assertTrue(e instanceof ContractValidateException);

      Assert.assertEquals("Invalid address", e.getMessage());

    } catch (ContractExeException e) {
      Assert.assertTrue(e instanceof ContractExeException);
    }

  }

  @Test
  public void invalidOwnerAccount() {
    WithdrawExpireUnfreezeActuator actuator = new WithdrawExpireUnfreezeActuator();
    actuator.setChainBaseManager(dbManager.getChainBaseManager())
        .setAny(getContract(OWNER_ACCOUNT_INVALID));

    TransactionResultCapsule ret = new TransactionResultCapsule();

    try {
      actuator.validate();
      actuator.execute(ret);
      fail("cannot run here.");
    } catch (ContractValidateException e) {
      Assert.assertTrue(e instanceof ContractValidateException);
      Assert.assertEquals("Account[" + OWNER_ACCOUNT_INVALID + "] not exists", e.getMessage());
    } catch (ContractExeException e) {
      Assert.assertFalse(e instanceof ContractExeException);
    }
  }

  @Test
  public void commonErrorCheck() {

    WithdrawExpireUnfreezeActuator actuator = new WithdrawExpireUnfreezeActuator();
    ActuatorTest actuatorTest = new ActuatorTest(actuator, dbManager);
    actuatorTest.noContract();

    Any invalidContractTypes = Any.pack(AssetIssueContractOuterClass.AssetIssueContract.newBuilder()
        .build());
    actuatorTest.setInvalidContract(invalidContractTypes);
    actuatorTest.setInvalidContractTypeMsg("contract type error",
        "contract type error, expected type [WithdrawExpireUnfreezeContract], real type[");
    actuatorTest.invalidContractType();

    long now = System.currentTimeMillis();
    dbManager.getDynamicPropertiesStore().saveLatestBlockHeaderTimestamp(now);
    byte[] address = ByteArray.fromHexString(OWNER_ADDRESS);
    try {
      dbManager.getMortgageService()
          .adjustAllowance(dbManager.getAccountStore(), address, allowance);
    } catch (BalanceInsufficientException e) {
      fail("BalanceInsufficientException");
    }
    AccountCapsule accountCapsule = dbManager.getAccountStore().get(address);
    Assert.assertEquals(accountCapsule.getAllowance(), allowance);
    Assert.assertEquals(accountCapsule.getLatestWithdrawTime(), 0);

    WitnessCapsule witnessCapsule = new WitnessCapsule(ByteString.copyFrom(address), 100,
        "http://google.com");
    dbManager.getWitnessStore().put(address, witnessCapsule);

    actuatorTest.setContract(getContract(OWNER_ADDRESS));
    actuatorTest.nullTransationResult();

    actuatorTest.setNullDBManagerMsg("No account store or dynamic store!");
    actuatorTest.nullDBManger();
  }

}

