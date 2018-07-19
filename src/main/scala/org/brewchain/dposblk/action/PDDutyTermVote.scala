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
import org.brewchain.dposblk.pbgens.Dposblock.PSCoMine
import org.brewchain.dposblk.pbgens.Dposblock.PRetCoMine
import org.brewchain.dposblk.pbgens.Dposblock.PCommand
import org.brewchain.dposblk.pbgens.Dposblock.PSDutyTermVote
import org.brewchain.dposblk.pbgens.Dposblock.PDutyTermResult
import org.brewchain.dposblk.tasks.DCtrl
import onight.tfw.otransio.api.PacketHelper
import org.apache.commons.lang3.StringUtils
import org.fc.brewchain.bcapi.exception.FBSException
import org.brewchain.dposblk.pbgens.Dposblock.PDutyTermResult.VoteResult
import org.brewchain.dposblk.tasks.BlockSync
import org.brewchain.dposblk.tasks.DTask_DutyTermVote
import org.brewchain.dposblk.utils.DConfig
import scala.collection.JavaConversions._
import scala.collection.mutable.Map
import org.brewchain.dposblk.pbgens.Dposblock.PDNode

@NActorProvider
@Instantiate
@Provides(specifications = Array(classOf[ActorService], classOf[IActor], classOf[CMDService]))
class PDDutyTermVote extends PSMDPoSNet[PSDutyTermVote] {
  override def service = PDDutyTermVoteService
}

//
// http://localhost:8000/fbs/xdn/pbget.do?bd=
object PDDutyTermVoteService extends LogHelper with PBUtils with LService[PSDutyTermVote] with PMNodeHelper {
  override def onPBPacket(pack: FramePacket, pbo: PSDutyTermVote, handler: CompleteHandler) = {
    //    log.debug("DPoS DutyTermVoteService::" + pack.getFrom())
    var ret = PDutyTermResult.newBuilder();
    val net = DCtrl.instance.network;
    if (!DCtrl.isReady() || net == null) {
      ret.setRetCode(-1).setRetMessage("DPoS Network Not READY")
      handler.onFinished(PacketHelper.toPBReturn(pack, ret.build()))
    } else {
      try {
        MDCSetBCUID(DCtrl.dposNet())
        MDCSetMessageID(pbo.getMessageId)
        val cn = DCtrl.curDN()
        val tm = DCtrl.termMiner();
        ret.setMessageId(pbo.getMessageId);
        ret.setBcuid(cn.getBcuid)
        ret.setRetCode(0).setRetMessage("SUCCESS")
        ret.setCurTermid(tm.getTermId).setCurBlock(cn.getCurBlock).setCurTermSign(tm.getSign)

        val vq = DCtrl.voteRequest();
        //
        ret.setTermId(pbo.getTermId)
        ret.setSign(pbo.getSign)
        ret.setCurTermSign(tm.getSign);
        ret.setVoteAddress(cn.getCoAddress)

        DTask_DutyTermVote.synchronized({
          if ((StringUtils.isBlank(tm.getSign) ||StringUtils.equals(pbo.getBcuid, cn.getBcuid)) ||
              (StringUtils.isBlank(vq.getSign) || vq.getTermId<=pbo.getTermId||//!!<=
                  StringUtils.equals(pbo.getBcuid, cn.getBcuid)) &&
            //            && StringUtils.isBlank(vq.getMessageId) || vq.getMessageId.equals(pbo.getLastTermUid))
            ((tm.getTermId <= pbo.getLastTermId) && tm.getTermId <= pbo.getTermId - 1
              && (
                (pbo.getBlockRange.getStartBlock >= tm.getBlockRange.getStartBlock && pbo.getRewriteTerm != null &&
                  pbo.getRewriteTerm.getBlockLost >= 0) //for revote
                  || pbo.getBlockRange.getStartBlock >= tm.getBlockRange.getEndBlock) // for continue vote
                  || StringUtils.equals(pbo.getCoAddress, cn.getCoAddress)) && pbo.getMinerQueueCount > 0) {
            //check quantifyminers
            val quantifyMinerByCoAddr = Map[String, PDNode]();
            DCtrl.coMinerByUID.filter(p =>
              if (//p._2.getBcuid.equals(cn.getBcuid) ||// for local ok
                  (p._2.getCurBlock >= cn.getCurBlock - DConfig.DTV_MUL_BLOCKS_EACH_TERM * (tm.getMinerQueueCount + 1) &&
                (tm.getTermId == p._2.getTermId || p._2.getTermId == tm.getLastTermId) &&
                (StringUtils.isBlank(tm.getSign) || StringUtils.equals(p._2.getTermSign, tm.getSign) ||
                  StringUtils.equals(p._2.getTermSign, tm.getLastTermUid)))
                  ) {
                true
              } else {
                log.debug("unquantifyminers:" + p._2.getBcuid + "," + p._2.getCoAddress + ",pblock=" + p._2.getCurBlock
                  + ",cn=" + cn.getCurBlock + ",PID=" + p._2.getTermId + ",TID=" + tm.getTermId + ",LTID=" + tm.getLastTermId
                  + ",pbtsign=" + p._2.getTermSign + ",tmsign=" + tm.getSign + ",lasttmsig=" + tm.getLastTermUid)
                false;
              }).map(f =>
              {
                quantifyMinerByCoAddr.put(f._2.getCoAddress, f._2);
              })
            val q = pbo.getMinerQueueList.filter { f =>
              if (!quantifyMinerByCoAddr.contains(f.getMinerCoaddr)) {
                //                log.debug("UNQuantifyNode:" + f.getMinerCoaddr);
                true;
              } else {
                false
              }
            }
            val reject =
              if (pbo.getBlockRange.getStartBlock != tm.getBlockRange.getEndBlock + 1 && tm.getTermId > 0
                && System.currentTimeMillis() - tm.getTermEndMs < DConfig.MAX_TIMEOUTSEC_FOR_REVOTE * 1000) {
                if (pbo.getRewriteTerm == null) {
                  log.debug("Reject DPos TermVote block not a sequence,cn.duty=" + cn.getDutyUid + ",T=" + pbo.getTermId
                    + ",VT=" + vq.getTermId + ",LT=" + pbo.getLastTermId
                    + ",TU=" + tm.getSign + ",LTM=" + tm.getLastTermUid
                    + ",PU=" + pbo.getSign + ",PTM=" + pbo.getLastTermUid
                    + ",VM=" + vq.getMessageId + ",LTM=" + pbo.getLastTermUid
                    + ",PA=" + pbo.getCoAddress + ",CA=" + cn.getCoAddress + ",qsize=" + q.size);
                  ret.setResult(VoteResult.VR_REJECT)
                  true
                } else {
                  //check rewrite
                  val (isMiner, isOverrided) = DCtrl.checkMiner(pbo.getBlockRange.getStartBlock, pbo.getCoAddress, System.currentTimeMillis())
                  if (!isOverrided || !isMiner) { //
                    log.debug("Not your Miner Voted!!isMiner=" + isMiner + ",isOverrided=" + isOverrided
                      + ",B=" + cn.getCurBlock + ",BS=[" + pbo.getBlockRange.getStartBlock + "," + pbo.getBlockRange.getEndBlock
                      + "],VM=" + vq.getMessageId + ",LTM=" + pbo.getLastTermUid
                      + ",PU=" + pbo.getSign + ",PTM=" + pbo.getLastTermUid
                      + ",TM=[" + tm.getBlockRange.getStartBlock + "," + tm.getBlockRange.getEndBlock
                      + "]");
                    ret.setResult(VoteResult.VR_REJECT)
                    true
                  } else {
                    //should be voting
                    false
                  }
                }
              } else {
                false
              }

            if (!reject) {
              if (pbo.getTermId == tm.getTermId + 1 && q.size > 0) {
                log.debug("Reject DPos TermVote Miner not quntified,cn.duty=" + cn.getDutyUid + ",PT=" + pbo.getTermId
                  + ",VT=" + vq.getTermId + ",LT=" + pbo.getLastTermId
                  + ",TU=" + tm.getSign + ",LTM=" + tm.getLastTermUid
                  + ",PU=" + pbo.getSign + ",PTM=" + pbo.getLastTermUid
                  + ",VM=" + vq.getMessageId + ",LTM=" + pbo.getLastTermUid
                  + ",PA=" + pbo.getCoAddress + ",CA=" + cn.getCoAddress + ",qsize=" + q.size);
                ret.setResult(VoteResult.VR_REJECT)
              } else {
                if (cn.getCurBlock < pbo.getBlockRange.getStartBlock - 1) {
                  log.debug("Grant DPos Term Vote but Block Height Not Ready:" + cn.getDutyUid + ",T=" + pbo.getTermId
                    + ",VT=" + vq.getTermId + ",LT=" + pbo.getLastTermId
                    + ",B=" + cn.getCurBlock + ",BS=[" + pbo.getBlockRange.getStartBlock + "," + pbo.getBlockRange.getEndBlock
                    + "],VM=" + vq.getMessageId + ",LTM=" + pbo.getLastTermUid
                    + ",PU=" + pbo.getSign + ",PTM=" + pbo.getLastTermUid
                    + ",PA=" + pbo.getCoAddress + ",CA=" + cn.getCoAddress + ",from=" + pbo.getBcuid);
                  ret.setResult(VoteResult.VR_GRANTED)
                  ret.setTermId(pbo.getTermId)
                  ret.setBcuid(cn.getBcuid)
                  ret.setSign(pbo.getSign)
                  ret.setVoteAddress(cn.getCoAddress)
                  DCtrl.instance.updateVoteReq(pbo);
                  //                  BlockSync.tryBackgroundSyncLogs(pbo.getBlockRange.getStartBlock - 1, pbo.getBcuid)(net)
                } else {
                  // 
                  log.debug("Grant DPos Term Vote:" + cn.getDutyUid + ",T=" + pbo.getTermId
                    + ",VT=" + vq.getTermId + ",LT=" + pbo.getLastTermId
                    + ",PBS=[" + pbo.getBlockRange.getStartBlock + "," + pbo.getBlockRange.getEndBlock + "]"
                    + ",TBS=[" + tm.getBlockRange.getStartBlock + "," + tm.getBlockRange.getEndBlock + "]"
                    + ",VBS=[" + vq.getBlockRange.getStartBlock + "," + vq.getBlockRange.getEndBlock + "]"
                    + ",VM=" + vq.getMessageId + ",LTM=" + pbo.getLastTermUid
                    + ",PA=" + pbo.getCoAddress + ",CA=" + cn.getCoAddress);
                  ret.setResult(VoteResult.VR_GRANTED)
                  ret.setTermId(pbo.getTermId)
                  ret.setSign(pbo.getSign)
                  ret.setBcuid(cn.getBcuid)

                  ret.setVoteAddress(cn.getCoAddress)
                  DCtrl.instance.updateVoteReq(pbo);
                }
              }
              //
            }
          } else {
            val nfino = if (pbo.getRewriteTerm != null) {
              "[" + pbo.getRewriteTerm.getBlockLost + "]"
            } else {
              "null";
            }
              log.debug("Reject DPos Term Vote: TM=" + tm.getSign + ",PT=" + pbo.getTermId
                + ",VT=" + vq.getTermId + ",PLT=" + pbo.getLastTermId + ",T=" + tm.getTermId
                + ",PBS=[" + pbo.getBlockRange.getStartBlock + "," + pbo.getBlockRange.getEndBlock + "]"
                + ",TBS=[" + tm.getBlockRange.getStartBlock + "," + tm.getBlockRange.getEndBlock + "]"
                + ",VBS=[" + vq.getBlockRange.getStartBlock + "," + vq.getBlockRange.getEndBlock + "]"
                + ",VM=" + vq.getMessageId + ",PLTU=" + pbo.getLastTermUid + ",LTU=" + tm.getLastTermUid
                + ",PA=" + pbo.getCoAddress + ",CA=" + cn.getCoAddress + ",ReWrite=" +
              nfino);
            ret.setResult(VoteResult.VR_REJECT)
            ret.setTermId(pbo.getTermId)
            ret.setSign(pbo.getSign)
            ret.setVoteAddress(cn.getCoAddress)
            //
          }

          DCtrl.instance.saveVoteReq(pbo);
          DTask_DutyTermVote.notifyAll()
        })

        net.dwallMessage("DTRDOB", Left(ret.build()), pbo.getMessageId);
        //        }

      } catch {
        case e: FBSException => {
          log.error("fbsException:" + e.getMessage, e);
          ret.clear()
          ret.setRetCode(-2).setRetMessage(e.getMessage)
        }
        case t: Throwable => {
          log.error("error:", t);
          ret.clear()
          ret.setRetCode(-3).setRetMessage("" + t.getMessage)
        }
      } finally {
        handler.onFinished(PacketHelper.toPBReturn(pack, ret.build()))

      }
    }
  }
  //  override def getCmds(): Array[String] = Array(PWCommand.LST.name())
  override def cmd: String = PCommand.DTV.name();
}
