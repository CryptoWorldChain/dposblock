package org.brewchain.dposblk.tasks

import org.fc.brewchain.p22p.node.Network
import org.fc.brewchain.p22p.utils.LogHelper
import onight.tfw.outils.serialize.UUIDGenerator
import scala.collection.JavaConversions._
import org.fc.brewchain.p22p.core.Votes
import org.fc.brewchain.p22p.core.Votes.Converge
import org.fc.brewchain.p22p.core.Votes.Undecisible
import org.brewchain.dposblk.utils.DConfig
import org.fc.brewchain.bcapi.JodaTimeHelper
import org.brewchain.dposblk.Daos
import org.brewchain.dposblk.pbgens.Dposblock.PDutyTermResult
import scala.collection.mutable.Buffer
import org.brewchain.dposblk.pbgens.Dposblock.PDutyTermResult.VoteResult
import org.brewchain.dposblk.pbgens.Dposblock.PSDutyTermVote
import org.brewchain.dposblk.pbgens.Dposblock.PSDutyTermVote.BlockRange
import org.brewchain.dposblk.pbgens.Dposblock.PSDutyTermVote.TermBlock

//获取其他节点的term和logidx，commitidx
object RTask_DutyTermVote extends LogHelper {
  def runOnce(implicit network: Network): Boolean = {
    Thread.currentThread().setName("RTask_RequestVote");
    val cn = DCtrl.curDN();
    val tm = DCtrl.termMiner();
    val vq = DCtrl.voteRequest()
    if (cn.getCurBlock + DConfig.DTV_BEFORE_BLK >= tm.getBlockRange.getEndBlock
      && vq.getBlockRange.getStartBlock >= tm.getBlockRange.getEndBlock) {
      // check db
      val records = Daos.dposdb.listBySecondKey("D" + vq.getTermId + "-" + vq.getSign)
      log.debug("check db status:B[=" + vq.getBlockRange.getStartBlock + ","
        + vq.getBlockRange.getEndBlock + "],T="
        + vq.getTermId + ",dbsize=" +
        records.get.size())
      if ((records.get.size() + 1) >= DCtrl.termMiner().getCoNodes * DConfig.VOTE_QUORUM_RATIO / 100) {
        log.debug("try to vote:" + records.get.size());
        val reclist: Buffer[PDutyTermResult.Builder] = records.get.map { p =>
          PDutyTermResult.newBuilder().mergeFrom(p.getValue.getExtdata);
        };
        Votes.vote(reclist).PBFTVote({ p =>
          Some(p.getResult)
        }, DCtrl.termMiner().getCoNodes) match {
          case Converge(n) =>
            log.debug("converge:" + n);
            if (n == VoteResult.VR_GRANTED) {
              log.debug("Vote Granted will be the new terms:" + n);
              DCtrl.instance.term_Miner = DCtrl.instance.vote_Request
              //RSM.instance.updateNodeState(RSM.curVR, RaftState.RS_LEADER)
              true
            } else if (n == VoteResult.VR_REJECT) {
              log.debug("Vote Not reject:" + n);
              //              RSM.resetVoteRequest();
              false
            } else {
              log.debug("unknow vote state")
              //              RSM.resetVoteRequest();
              false
            }
          case n: Undecisible =>
            if (records.get.size() == DCtrl.termMiner().getCoNodes - 1) {
              log.debug("Undecisible but not converge.")
              //          !!    RSM.resetVoteRequest();
            } else {
              log.debug("cannot decide vote state, wait other response")
            }
            false
          case _ =>
            log.debug("not converge,try next time")
            //            RSM.resetVoteRequest();
            false
        }
      } else {
        false
      }
    } else if (cn.getCurBlock + DConfig.DTV_BEFORE_BLK >= tm.getBlockRange.getEndBlock
      || JodaTimeHelper.secondIntFromNow(tm.getTermEndMs) > DConfig.DTV_TIMEOUT_SEC) {
      val msgid = UUIDGenerator.generate();
      MDCSetMessageID(msgid);

      DCtrl.coMinerByUID.filter(p => {
        network.nodeByBcuid(p._1) == network.noneNode
      }).map { p =>
        log.debug("remove Node:" + p._1);
        DCtrl.coMinerByUID.remove(p._1);
      }

      val newterm = DCtrl.instance.term_Miner.clone();
      val conodescount = Math.min(DCtrl.coMinerByUID.size, DConfig.DTV_MAX_SUPER_MINER);
      
      newterm.setBlockRange(BlockRange.newBuilder()
        .setStartBlock(tm.getBlockRange.getEndBlock + 1)
        .setEndBlock(tm.getBlockRange.getEndBlock +
          DConfig.DTV_MUL_BLOCKS_EACH_TERM *
          conodescount))
        .setCoNodes(conodescount)
        .setMessageId(msgid)
        .setCoAddress(DCtrl.instance.cur_dnode.getCoAddress)
        .setCwsGuaranty(1)
        .setSliceId(1)
        .setTermStartMs(System.currentTimeMillis());
      newterm.setTermEndMs(DConfig.DTV_TIME_MS_EACH_BLOCK *
        (newterm.getBlockRange.getEndBlock -
          newterm.getBlockRange.getStartBlock));
      newterm.setTermId(tm.getTermId + 1)
      .setLastTermId(tm.getTermId)
      .setLastTermUid(tm.getMessageId)

      val rand = Math.random() * 1000
      val rdns = scala.util.Random.shuffle(DCtrl.coMinerByUID);
      log.debug(" rdns=" + rdns);
      var i = newterm.getBlockRange.getStartBlock;
      rdns.map { x =>
        if (newterm.getMinerQueueCount < conodescount) {
          newterm.addMinerQueue(TermBlock.newBuilder().setBlockHeight(i)
            .setMinerCoaddr(x._2.getCoAddress))
          i = i + 1;
        }
      }
      log.debug("mineQ=" + newterm.getMinerQueueList)

      log.debug("get coMinerNodeCount=" + DCtrl.coMinerByUID.size + ",NetworkDNodecount=" + network.directNodeByBcuid.size);

      //checking health remove offline nodes.

      val curtime = System.currentTimeMillis()
      DCtrl.instance.vote_Request = newterm;
      log.debug("try to vote:newterm=" + newterm.getTermId + ",curterm=" + tm.getTermId
        + ",voteN=" + conodescount)
      network.wallOutsideMessage("DTVDOB", Left(DCtrl.voteRequest().build()), msgid);
      false
    } else {
      false
    }
  }

}
