package org.brewchain.dposblk.action

import org.apache.commons.codec.binary.Hex
import org.apache.felix.ipojo.annotations.Instantiate
import org.apache.felix.ipojo.annotations.Provides
import onight.tfw.ntrans.api.ActorService
import onight.tfw.proxy.IActor
import onight.tfw.otransio.api.session.CMDService
import onight.osgi.annotation.NActorProvider
import org.brewchain.dposblk.PSMDPoSNet
import org.fc.brewchain.p22p.utils.LogHelper
import onight.oapi.scala.commons.PBUtils
import onight.oapi.scala.commons.LService
import org.fc.brewchain.p22p.action.PMNodeHelper
import onight.tfw.otransio.api.beans.FramePacket
import onight.tfw.async.CompleteHandler
import org.brewchain.bcapi.utils.PacketIMHelper._
import org.brewchain.dposblk.pbgens.Dposblock.PSCoMine
import org.brewchain.dposblk.pbgens.Dposblock.PRetCoMine
import org.brewchain.dposblk.pbgens.Dposblock.PCommand
import org.brewchain.dposblk.pbgens.Dposblock.PSSyncTransaction
import org.brewchain.dposblk.pbgens.Dposblock.PRetSyncTransaction
import org.brewchain.dposblk.tasks.DCtrl
import onight.tfw.otransio.api.PacketHelper
import org.fc.brewchain.bcapi.exception.FBSException
import org.brewchain.dposblk.Daos
import org.brewchain.evmapi.gens.Tx.MultiTransaction

import scala.collection.JavaConversions._
import org.apache.commons.lang3.StringUtils
import java.util.ArrayList
import java.math.BigInteger
import org.brewchain.dposblk.pbgens.Dposblock.PSSyncTransaction.SyncType
import org.brewchain.account.bean.HashPair
import onight.tfw.outils.serialize.UUIDGenerator

@NActorProvider
@Instantiate
@Provides(specifications = Array(classOf[ActorService], classOf[IActor], classOf[CMDService]))
class PDPoSTransactionSync extends PSMDPoSNet[PSSyncTransaction] {
  override def service = PDPoSTransactionSyncService
}

object PDPoSTransactionSyncService extends LogHelper with PBUtils with LService[PSSyncTransaction] with PMNodeHelper {
  override def onPBPacket(pack: FramePacket, pbo: PSSyncTransaction, handler: CompleteHandler) = {
    var ret = PRetSyncTransaction.newBuilder();
    if (!DCtrl.isReady()) {
      ret.setRetCode(-1).setRetMessage("DPoS Network Not READY")
      handler.onFinished(PacketHelper.toPBReturn(pack, ret.build()))
    } else {
      try {
        MDCSetBCUID(DCtrl.dposNet())
        MDCSetMessageID(pbo.getMessageid)

        var bits = new BigInteger("" + DCtrl.instance.network.root().node_idx);
        val confirmNode =
          pbo.getSyncType match {
            case SyncType.ST_WALLOUT =>
              DCtrl.instance.network.nodeByBcuid(pbo.getFromBcuid);
            case _ =>
              DCtrl.instance.network.nodeByBcuid(pbo.getConfirmBcuid);
          }

        if (confirmNode != DCtrl.instance.network.noneNode) {

          bits = bits.setBit(confirmNode.node_idx);
          pbo.getSyncType match {
            case SyncType.ST_WALLOUT =>
              val dbsaveList = new ArrayList[MultiTransaction.Builder]();
              for (x <- pbo.getTxDatasList) {
                var oMultiTransaction = MultiTransaction.newBuilder();
                oMultiTransaction.mergeFrom(x);
                if (!StringUtils.equals(DCtrl.curDN().getBcuid, oMultiTransaction.getTxNode().getBcuid)) {
                  //            Daos.txHelper.syncTransaction(oMultiTransaction);
                  dbsaveList.add(oMultiTransaction)
                }
                //批量入库
              }
              Daos.txHelper.syncTransaction(dbsaveList, bits);

              //resend
              val msgid = UUIDGenerator.generate();
              val syncTransaction = PSSyncTransaction.newBuilder();
              syncTransaction.setMessageid(msgid);
              syncTransaction.setSyncType(SyncType.ST_CONFIRM_RECV);
              syncTransaction.setFromBcuid(pbo.getFromBcuid);
              syncTransaction.setConfirmBcuid(DCtrl.instance.network.root().bcuid)
              syncTransaction.addAllTxHash(pbo.getTxHashList);
              //      syncTransaction.addAllTxHash(res.getTxHashList);
              //      syncTransaction.addAllTxDatas(res.getTxDatasList);
              DCtrl.instance.network.wallMessage("BRTDOB", Left(syncTransaction.build()), msgid)

            case _ =>
              pbo.getTxHashList.map { txHash =>
                Daos.txHelper.confirmRecvTx(Hex.encodeHexString(txHash.toByteArray()), bits);
              }
          }
        } else {
          log.debug("cannot find bcuid from network:" + pbo.getConfirmBcuid + "," + pbo.getFromBcuid + ",synctype="+pbo.getSyncType);
        }

        ret.setRetCode(1)
      } catch {
        case t: Throwable => {
          log.error("error:", t);
          ret.clear()
          ret.setRetCode(-3).setRetMessage(t.getMessage)
        }
      } finally {
        handler.onFinished(PacketHelper.toPBReturn(pack, ret.build()))
      }
    }
  }
  override def cmd: String = PCommand.BRT.name();
}