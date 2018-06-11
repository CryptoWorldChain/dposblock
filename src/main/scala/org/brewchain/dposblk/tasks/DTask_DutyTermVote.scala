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
import org.apache.commons.lang3.StringUtils
import org.brewchain.bcapi.gens.Oentity.OPair
import java.util.concurrent.Future
import org.fc.brewchain.p22p.core.Votes.NotConverge
import scala.collection.mutable.Map
import org.brewchain.bcapi.gens.Oentity.OKey
import com.google.protobuf.ByteString
import org.apache.commons.codec.binary.Hex
import org.brewchain.dposblk.pbgens.Dposblock.PSDutyTermVote.RewriteTerm
import org.brewchain.dposblk.pbgens.Dposblock.PDNode

//获取其他节点的term和logidx，commitidx
object DTask_DutyTermVote extends LogHelper {

  var ban_for_vote_sec = 0L;
  def sleepToNextVote(): Unit = {
    val ban_sec = (Math.abs(Math.random() * 100000 % (DConfig.BAN_MAXSEC_FOR_VOTE_REJECT - DConfig.BAN_MINSEC_FOR_VOTE_REJECT)) +
      DConfig.BAN_MINSEC_FOR_VOTE_REJECT).asInstanceOf[Long]
    //    log.debug("Undecisible but not converge.ban sleep=" + ban_sec)
    log.debug("ban for vote sleep:" + ban_sec + " seconds");
    ban_for_vote_sec = System.currentTimeMillis() + ban_sec * 1000;
    //DTask_DutyTermVote.synchronized({
    //      DTask_DutyTermVote.wait(ban_sec * 1000)
    //    })
  }
  val omitNodes: Map[String, PDNode] = Map.empty;
  def clearRecords(votelist: Buffer[PDutyTermResult.Builder]): Unit = {
    Daos.dposdb.batchDelete(votelist.map { p =>
      OKey.newBuilder().setData(
        ByteString.copyFromUtf8(
          "V" + p.getTermId + "-" + p.getSign + "-"
            + p.getBcuid)).build()
    }.toArray)
  }
  def checkVoteDB(vq: PSDutyTermVote.Builder)(implicit network: Network): Boolean = {
    val records = Daos.dposdb.listBySecondKey("D" + vq.getTermId)
    log.debug("check db status:B[=" + vq.getBlockRange.getStartBlock + ","
      + vq.getBlockRange.getEndBlock + "],T="
      + vq.getTermId
      + ",sign=" + vq.getSign
      + ",N=" + vq.getCoNodes
      + ",dbsize=" +
      records.get.size())
    if (records.get.size() == 0) {
      DCtrl.voteRequest().clear()
      false
    } else if ((records.get.size() + 1) >= vq.getCoNodes * DConfig.VOTE_QUORUM_RATIO / 100
      || (System.currentTimeMillis() - vq.getTermStartMs > DConfig.MAX_TIMEOUTSEC_FOR_REVOTE * 1000)) {
      //      log.debug("try to vote:" + records.get.size());
      val reclist: Buffer[PDutyTermResult.Builder] = records.get.map { p =>
        PDutyTermResult.newBuilder().mergeFrom(p.getValue.getExtdata);
      };
      val realist = reclist.filter { p => DCtrl.coMinerByUID.containsKey(p.getBcuid) };
      log.debug("check db status:B[=" + vq.getBlockRange.getStartBlock + ","
        + vq.getBlockRange.getEndBlock + "],T="
        + vq.getTermId
        + ",sign=" + vq.getSign
        + ",N=" + vq.getCoNodes
        + ",dbsize=" + records.get.size()
        + ",realsize=" + realist.size())
      val signmap = Map[String, Buffer[PDutyTermResult.Builder]]();
      realist.map { x =>
        val buff = signmap.get(x.getSign) match {
          case Some(_buffn) => _buffn
          case None => {
            val r = Buffer[PDutyTermResult.Builder]();
            signmap.put(x.getSign, r)
            r
          }
        }
        buff.append(x)
      }
      var hasConverge = false;
      var banForLocal = false;
      signmap.map { kv =>
        val sign = kv._1
        val votelist = kv._2
        val dbtempvote = DCtrl.instance.loadVoteReq(sign);
        log.debug("dbtempvote=" + dbtempvote.getSign + ",vid=" + dbtempvote.getTermId + ",TID=" + DCtrl.termMiner().getTermId + ",sign=" + sign + ",size=" + votelist.size);
        if (!hasConverge && StringUtils.equals(dbtempvote.getSign, sign)) {
          if (dbtempvote.getTermId > DCtrl.termMiner().getTermId) {
            val result = Votes.vote(votelist).PBFTVote({ p =>
              Some(p.getResult)
            }, dbtempvote.getCoNodes) match {
              case Converge(n) =>
                //          log.debug("converge:" + n); 
                if (n == VoteResult.VR_GRANTED) {
                  log.debug("Vote Granted will be the new terms:T="
                    + dbtempvote.getTermId
                    + ",curr=" + DCtrl.curDN().getCoAddress
                    + ",sign=" + dbtempvote.getSign
                    + ",N=" + dbtempvote.getCoNodes + ":"
                    + dbtempvote.getMinerQueueList.foldLeft(",")((a, b) => a + "," + b.getBlockHeight + "=" + b.getMinerCoaddr));
                  DCtrl.instance.term_Miner = dbtempvote
                  DCtrl.instance.updateTerm()
                  hasConverge = true;
                  true
                } else if (n == VoteResult.VR_REJECT) {
                  clearRecords(votelist);
                  if (StringUtils.equals(dbtempvote.getCoAddress, DCtrl.instance.cur_dnode.getCoAddress)) {
                    banForLocal = true;
                  }
                  false
                } else {
                  clearRecords(votelist);
                  if (StringUtils.equals(dbtempvote.getCoAddress, DCtrl.instance.cur_dnode.getCoAddress)) {
                    banForLocal = true;
                  }
                  false
                }
              case n: Undecisible =>
                log.debug("Undecisible:dbsize=" + votelist.size + ",N=" + dbtempvote.getCoNodes);
                if (System.currentTimeMillis() - dbtempvote.getTermStartMs > DConfig.MAX_TIMEOUTSEC_FOR_REVOTE * 1000) {
                  log.debug("clear timeout vote after:" + JodaTimeHelper.secondFromNow(dbtempvote.getTermStartMs))
                  clearRecords(votelist);
                }
                false
              case n: NotConverge =>
                clearRecords(votelist);
                if (StringUtils.equals(dbtempvote.getCoAddress, DCtrl.instance.cur_dnode.getCoAddress)) {
                  banForLocal = true;
                }
                false
              case a @ _ =>
                //              clearRecords(votelist);
                false
            }
            if (result) {
              hasConverge = result
            }
          } else { //unclean data
            clearRecords(votelist);
          }
        }
      }
      if (!hasConverge && banForLocal) {
        DCtrl.voteRequest().clear()
        DCtrl.curDN().clearDutyUid();
        sleepToNextVote();
      }
      hasConverge
    } else {

      log.debug("check status Not enough results:B[=" + vq.getBlockRange.getStartBlock + ","
        + vq.getBlockRange.getEndBlock + "],T="
        + vq.getTermId
        + ",sign=" + vq.getSign
        + ",N=" + vq.getCoNodes
        + ",dbsize=" + records.get.size())
      false
    }
  }
  def runOnce(implicit network: Network): Boolean = {
    Thread.currentThread().setName("RTask_RequestVote");
    DTask_DutyTermVote.synchronized({
      val cn = DCtrl.curDN();
      val tm = DCtrl.termMiner();
      val vq = DCtrl.voteRequest()
      log.debug("dutyvote:vq.tid=" + vq.getTermId + ",B=" + cn.getCurBlock + ",vq[" + vq.getBlockRange.getStartBlock + "," + vq.getBlockRange.getEndBlock + "]"
        + ",tm[" + tm.getBlockRange.getStartBlock + "," + tm.getBlockRange.getEndBlock + "]"
        + ",vq.lasttermuid=" + vq.getLastTermUid + ",tm.sign=" + tm.getSign + ",vqsign=" + vq.getSign + ",tid=" + tm.getTermId + ",banVote=" + (System.currentTimeMillis() <= ban_for_vote_sec))
      if ((cn.getCurBlock + DConfig.DTV_BEFORE_BLK >= tm.getBlockRange.getEndBlock
        && vq.getBlockRange.getStartBlock >= tm.getBlockRange.getEndBlock
        && vq.getBlockRange.getStartBlock >= cn.getCurBlock
        && vq.getTermId > 0
        || (StringUtils.isNotBlank(vq.getLastTermUid) && vq.getLastTermId.equals(tm.getTermId)
          && tm.getTermId > 0) && JodaTimeHelper.secondIntFromNow(vq.getTermStartMs) <= DConfig.DTV_TIMEOUT_SEC)) {
        checkVoteDB(vq)
      } else if ((cn.getCurBlock + DConfig.DTV_BEFORE_BLK >= tm.getBlockRange.getEndBlock
        || JodaTimeHelper.secondIntFromNow(tm.getTermEndMs) > DConfig.DTV_TIMEOUT_SEC)
        && System.currentTimeMillis() > ban_for_vote_sec &&
        (cn.getCominerStartBlock +
          Math.min(DConfig.COMINER_WAIT_BLOCKS_TODUTY, (tm.getCoNodes - 1) *
            DConfig.DTV_MUL_BLOCKS_EACH_TERM) <= cn.getCurBlock)) {

        val msgid = UUIDGenerator.generate();
        MDCSetMessageID(msgid);
        val canvote = if (JodaTimeHelper.secondIntFromNow(tm.getTermEndMs) < DConfig.DTV_TIMEOUT_SEC &&
          StringUtils.isNotBlank(tm.getSign) && tm.getCoNodes > 1) {
          val idx = (Math.abs(tm.getSign.hashCode()) % tm.getMinerQueueCount)
          tm.getMinerQueue(idx).getMinerCoaddr.equals(cn.getCoAddress)
        } else {
          true
        }
        if (canvote) {
          //      log.debug("try vote new term:");
          VoteTerm(network);
          false
        } else {
          log.debug("cannot vote Sec=" + JodaTimeHelper.secondIntFromNow(tm.getTermEndMs) + ",DV=" + DConfig.DTV_TIMEOUT_SEC
            + ",co" + tm.getCoNodes);
          false
        }
      } else {
        checkVoteDB(vq)
      }
    })
  }
  def VoteTerm(implicit network: Network, omitCoaddr: String = "", overridedBlock: Int = 0): Unit = {

    val msgid = UUIDGenerator.generate();
    MDCSetMessageID(msgid);
    DCtrl.coMinerByUID.filter(p => {
      network.nodeByBcuid(p._1) == network.noneNode || StringUtils.equals(omitCoaddr, p._2.getCoAddress)
    }).map { p =>
      log.debug("remove Node:" + p._1);
      DCtrl.coMinerByUID.remove(p._1);
    }
    val cn = DCtrl.curDN();
    val tm = DCtrl.termMiner();
    val vq = DCtrl.voteRequest()

    val quantifyminers = DCtrl.coMinerByUID.filter(p =>

      if (!StringUtils.equals(omitCoaddr, p._2.getCoAddress) &&
        (p._2.getCurBlock >= cn.getCurBlock - DConfig.DTV_MUL_BLOCKS_EACH_TERM - tm.getMinerQueueCount &&
          (StringUtils.isBlank(tm.getSign) || StringUtils.equals(p._2.getTermSign, tm.getSign) ||
            StringUtils.equals(p._2.getTermSign, tm.getLastTermUid)))) {
        true
      } else {
        log.debug("remove unquantifyminers:" + p._2.getBcuid + "," + p._2.getCoAddress + ",pblock=" + p._2.getCurBlock
          + ",cn=" + cn.getCurBlock
          + ",pbtsign=" + p._2.getTermSign + ",tmsign=" + tm.getSign + ",lasttmsig=" + tm.getLastTermUid)
        false;
      })
    if (quantifyminers.size > 0) {
      val newterm = PSDutyTermVote.newBuilder();
      val conodescount = Math.min(quantifyminers.size, DConfig.DTV_MAX_SUPER_MINER);
      val mineBlockCount = DConfig.DTV_MUL_BLOCKS_EACH_TERM * conodescount;
      val startBlk = cn.getCurBlock + 1;
      newterm.setBlockRange(BlockRange.newBuilder()
        .setStartBlock(startBlk)
        .setEndBlock(startBlk + mineBlockCount - 1)
        .setEachBlockMs(DConfig.BLK_EPOCH_MS))
        .setCoNodes(DCtrl.coMinerByUID.size)
        .setMessageId(msgid)
        .setCoAddress(DCtrl.instance.cur_dnode.getCoAddress)
        .setCwsGuaranty(DConfig.MAX_CWS_GUARANTY)
        .setSliceId(1)
        .setMaxTnxEachBlock(DConfig.MAX_TNX_EACH_BLOCK)
        .setBcuid(cn.getBcuid)
        .setTermStartMs(System.currentTimeMillis());

      if (overridedBlock > 0 && StringUtils.isNotBlank(omitCoaddr)) {
        newterm.setCoNodes(newterm.getCoNodes - DCtrl.coMinerByUID.filter(p => p._2.getCoAddress.equals(omitCoaddr)).size)
        log.debug("overrideBlockedVote!!TID=" + tm.getTermId + ",Tuid=" + tm.getSign + ",block=" + overridedBlock + ",cur=" + cn.getCurBlock);
        newterm.setRewriteTerm(RewriteTerm.newBuilder().setBlockLost(overridedBlock)
          .setRewriteMs(System.currentTimeMillis()).setTermStartMs(tm.getTermStartMs))

      }
      newterm.setTermEndMs(DConfig.DTV_TIME_MS_EACH_BLOCK * mineBlockCount);

      newterm.setTermId(tm.getTermId + 1)
        .setLastTermId(tm.getTermId)
        .setLastTermUid(tm.getSign)
        .setSign(msgid)

      val rand = Math.random() * 1000
      val rdns = scala.util.Random.shuffle(quantifyminers);
      //      log.debug(" rdns=" + rdns.foldLeft("")((a, b) => a + "," + b._1));
      var i = newterm.getBlockRange.getStartBlock;
      var bitcc = BigInt(0);

      while (newterm.getMinerQueueCount < mineBlockCount) {
        rdns.map { x =>
          if (newterm.getMinerQueueCount < mineBlockCount) {
            //              log.debug(" add miner at Queue," + x._2.getCoAddress + ",blockheight=" + i);
            newterm.addMinerQueue(TermBlock.newBuilder().setBlockHeight(i)
              .setMinerCoaddr(x._2.getCoAddress))
            i = i + 1;
          }
        }
      }

      log.debug("try to vote:newterm=" + newterm.getTermId + ",curterm=" + tm.getTermId
        + ",vN=" + DCtrl.coMinerByUID.size + ",cN=" + conodescount + ",sign=" + newterm.getSign + ",mineQ=" + newterm.getMinerQueueList.foldLeft(",")((a, b) => a + "," + b.getBlockHeight + "=" + b.getMinerCoaddr))
      DCtrl.instance.vote_Request = newterm;
      network.dwallMessage("DTVDOB", Left(DCtrl.voteRequest().build()), msgid);
    } else {
      log.debug("No more quaitify node can vote:");
    }
  }

}
