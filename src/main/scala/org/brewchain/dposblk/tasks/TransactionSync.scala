package org.brewchain.dposblk.tasks

import org.fc.brewchain.p22p.node.Network
import org.fc.brewchain.p22p.utils.LogHelper
import onight.tfw.outils.serialize.UUIDGenerator
import onight.tfw.async.CallBack
import onight.tfw.otransio.api.beans.FramePacket

import scala.collection.JavaConversions._
import org.brewchain.bcapi.gens.Oentity.OValue
import java.util.concurrent.CountDownLatch
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.TimeUnit
import org.brewchain.dposblk.utils.DConfig
import org.fc.brewchain.p22p.node.Networks
import java.util.concurrent.atomic.AtomicBoolean
import org.brewchain.dposblk.pbgens.Dposblock.PSSyncTransaction
import org.brewchain.dposblk.Daos
import org.brewchain.bcapi.exec.SRunner
import org.fc.brewchain.p22p.action.PMNodeHelper

case class TransactionSync(network: Network) extends SRunner with PMNodeHelper with LogHelper {
  def getName() = "TxSync"
  def runOnce() = {
    Thread.currentThread().setName("TxSync");
    TxSync.trySyncTx(network);
  }
}
object TxSync extends LogHelper {
  var instance: TransactionSync = TransactionSync(null);
  def dposNet(): Network = instance.network;

  def trySyncTx(network: Network): Unit = {

    val res = Daos.txHelper.getWaitSendTxToSend(DConfig.MAX_TNX_EACH_BROADCAST)
    if (res.getTxHashCount > 0) {
      val msgid = UUIDGenerator.generate();
      val syncTransaction = PSSyncTransaction.newBuilder();
      syncTransaction.setMessageid(msgid);

      for (x <- res.getTxHashList) {
        syncTransaction.addTxHash(x)
      }

      for (x <- res.getTxDatasList) {
        syncTransaction.addTxDatas(x)
      }

      //      syncTransaction.addAllTxHash(res.getTxHashList);
      //      syncTransaction.addAllTxDatas(res.getTxDatasList);
      network.dwallMessage("BRTDOB", Left(syncTransaction.build()), msgid)
    } else {
      //        log.debug("not found transaction for broadcast:" + res.getTxHexStrCount())
    }
  }
}