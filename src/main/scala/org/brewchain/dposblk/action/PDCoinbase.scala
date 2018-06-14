package org.brewchain.dposblk.action

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
import org.brewchain.dposblk.pbgens.Dposblock.PCommand
import org.brewchain.dposblk.pbgens.Dposblock.PSCoinbase
import org.brewchain.dposblk.pbgens.Dposblock.PRetCoMine
import org.brewchain.dposblk.pbgens.Dposblock.PRetCoinbase
import org.brewchain.dposblk.tasks.DCtrl
import onight.tfw.otransio.api.PacketHelper
import org.brewchain.dposblk.pbgens.Dposblock.PRetCoinbase.CoinbaseResult
import org.fc.brewchain.bcapi.exception.FBSException
import org.apache.commons.lang3.StringUtils
import org.brewchain.dposblk.pbgens.Dposblock.PBlockEntry
import org.brewchain.dposblk.tasks.BlockSync
import org.brewchain.dposblk.tasks.DTask_DutyTermVote
import org.brewchain.dposblk.utils.DConfig

@NActorProvider
@Instantiate
@Provides(specifications = Array(classOf[ActorService], classOf[IActor], classOf[CMDService]))
class PDCoinbaseM extends PSMDPoSNet[PSCoinbase] {
  override def service = PDCoinbase
}

//
// http://localhost:8000/fbs/xdn/pbget.do?bd=
object PDCoinbase extends LogHelper with PBUtils with LService[PSCoinbase] with PMNodeHelper {
  override def onPBPacket(pack: FramePacket, pbo: PSCoinbase, handler: CompleteHandler) = {
    //    log.debug("Mine Block From::" + pack.getFrom())
    var ret = PRetCoinbase.newBuilder();
    if (!DCtrl.isReady()) {
      log.debug("DCtrl not ready");
      ret.setRetCode(-1).setRetMessage("DPoS Network Not READY")
      handler.onFinished(PacketHelper.toPBReturn(pack, ret.build()))
    } else {
      try {
        MDCSetBCUID(DCtrl.dposNet())
        MDCSetMessageID(pbo.getMessageId)
        ret.setMessageId(pbo.getMessageId);
        //
        val cn = DCtrl.curDN()
        ret.setRetCode(0).setRetMessage("SUCCESS")
        if (!StringUtils.equals(pbo.getCoAddress, cn.getCoAddress)) {
          cn.synchronized {
            if (StringUtils.equals(pbo.getCoAddress, cn.getCoAddress) || pbo.getBlockHeight > cn.getCurBlock) {
              if (pbo.getTermId > DCtrl.termMiner().getTermId ||
                DCtrl.checkMiner(pbo.getBlockHeight, pbo.getCoAddress, pbo.getMineTime)._1) {
                DCtrl.saveBlock(pbo.getBlockEntry) match {
                  case n if n > 0 && n < pbo.getBlockHeight =>
                    ret.setResult(CoinbaseResult.CR_PROVEN)
                    log.info("newblock:UU,H=" + pbo.getBlockHeight + ",DB=" + n + ":coadr=" + pbo.getCoAddress + ",MN=" + DCtrl.coMinerByUID
                      .size + ",DN=" + DCtrl.dposNet().directNodeByIdx.size + ",PN=" + DCtrl.dposNet().pendingNodeByBcuid.size+",CN="+DCtrl.termMiner().getCoNodes);
                    BlockSync.tryBackgroundSyncLogs(pbo.getBlockHeight, pbo.getBcuid)(DCtrl.dposNet())
                  case n if n > 0 =>
                    log.info("newblock:OK,H=" + pbo.getBlockHeight + ",DB=" + n + ":coadr=" + pbo.getCoAddress + ",MN=" + DCtrl.coMinerByUID
                      .size + ",DN=" + DCtrl.dposNet().directNodeByIdx.size + ",PN=" + DCtrl.dposNet().pendingNodeByBcuid.size+",CN="+DCtrl.termMiner().getCoNodes)
                    ret.setResult(CoinbaseResult.CR_PROVEN)
                  case n @ _ =>
                    log.info("newblock:NO,H=" + pbo.getBlockHeight + ",DB=" + n + ":coadr=" + pbo.getCoAddress + ",MN=" + DCtrl.coMinerByUID
                      .size + ",DN=" + DCtrl.dposNet().directNodeByIdx.size + ",PN=" + DCtrl.dposNet().pendingNodeByBcuid.size+",CN="+DCtrl.termMiner().getCoNodes)
                    ret.setResult(CoinbaseResult.CR_REJECT)
                }
              } else {
                log.debug("Miner not for the block:Block=" + pbo.getBlockHeight + ",CA=" + pbo.getCoAddress + ",sign=" + pbo.getBlockEntry.getSign + ",from=" + pbo.getBcuid);
                ret.setResult(CoinbaseResult.CR_REJECT)
              }
            } else {
              log.debug("Current Miner Height is not consequence,PBOH=" + pbo.getBlockHeight + ",CUR=" + cn.getCurBlock
                + ",CA=" + pbo.getCoAddress + ",sign=" + pbo.getBlockEntry.getSign + ",from=" + pbo.getBcuid);
              ret.setResult(CoinbaseResult.CR_REJECT)
            }
            if (pbo.getTermId > DCtrl.termMiner().getTermId) {
              log.debug("local term id lower than block:pbot=" + pbo.getTermId + ",tm=" + DCtrl.termMiner().getTermId + ",H=" + pbo.getBlockHeight + ",DBH=" + cn.getCurBlock + ":coadrr=" + pbo.getCoAddress + ",MN=" + DCtrl.coMinerByUID.size + ",DN=" + DCtrl.dposNet().directNodeByIdx.size + ",PN=" + DCtrl.dposNet().pendingNodeByBcuid.size);
              if (DTask_DutyTermVote.possibleTermID.size() < DConfig.MAX_POSSIBLE_TERMID) {
                DTask_DutyTermVote.possibleTermID.put(pbo.getTermId, pbo.getBcuid);
              }
            }
          }
        } else {
          log.info("newblock:ok,H=" + pbo.getBlockHeight + ",DB=" + pbo.getBlockHeight + ":Local=" + pbo.getCoAddress + ",MN=" + DCtrl.coMinerByUID
            .size + ",DN=" + DCtrl.dposNet().directNodeByIdx.size + ",PN=" + DCtrl.dposNet().pendingNodeByBcuid.size+",CN="+DCtrl.termMiner().getCoNodes)
          ret.setResult(CoinbaseResult.CR_PROVEN)
        }

      } catch {
        case e: FBSException => {
          ret.clear()
          ret.setRetCode(-2).setRetMessage(e.getMessage)
        }
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
  //  override def getCmds(): Array[String] = Array(PWCommand.LST.name())
  override def cmd: String = PCommand.MIN.name();
}
