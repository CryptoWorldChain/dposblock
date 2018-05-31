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

//获取其他节点的term和logidx，commitidx
object DTask_DutyTermVote extends LogHelper {

  def sleepToNextVote(): Unit = {
    val ban_sec = (Math.abs(Math.random() * 100000 % (DConfig.BAN_MAXSEC_FOR_VOTE_REJECT - DConfig.BAN_MINSEC_FOR_VOTE_REJECT)) +
      DConfig.BAN_MINSEC_FOR_VOTE_REJECT).asInstanceOf[Long]
    //    log.debug("Undecisible but not converge.ban sleep=" + ban_sec)
    log.debug("ban for vote sleep:" + ban_sec + " seconds");
    DTask_DutyTermVote.synchronized({
      DTask_DutyTermVote.wait(ban_sec * 1000)
    })
  }
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
    //    log.debug("check db status:B[=" + vq.getBlockRange.getStartBlock + ","
    //      + vq.getBlockRange.getEndBlock + "],T="
    //      + vq.getTermId
    //      + ",sign=" + vq.getSign
    //      + ",N=" + vq.getCoNodes
    //      + ",dbsize=" +
    //      records.get.size())
    if (records.get.size() == 0) {
      DCtrl.voteRequest().clear()
      false
    } else if ((records.get.size() + 1) >= vq.getCoNodes * DConfig.VOTE_QUORUM_RATIO / 100) {
      log.debug("try to vote:" + records.get.size());
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
        log.debug("dbtempvote=" + dbtempvote.getSign + ",sign=" + sign + ",size=" + votelist.size);
        if (!hasConverge && StringUtils.equals(dbtempvote.getSign, sign)) {
          val result = Votes.vote(votelist).PBFTVote({ p =>
            Some(p.getResult)
          }, dbtempvote.getCoNodes) match {
            case Converge(n) =>
              //          log.debug("converge:" + n); 
              if (n == VoteResult.VR_GRANTED) {
                log.debug("Vote Granted will be the new terms:T="
                  + dbtempvote.getTermId
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
              log.debug("Undecisible:dbsize=" + votelist + ",N=" + dbtempvote.getCoNodes);
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
      if (!hasConverge && banForLocal) {
        DCtrl.voteRequest().clear()
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
      log.debug("dutyvote:vq.tid=" + vq.getTermId + "," + vq.getBlockRange.getStartBlock + "," + vq.getBlockRange.getEndBlock + "]"
        + ",vq.lasttermuid=" + vq.getLastTermUid + ",tm.termid=" + vq.getTermId)
      if (cn.getCurBlock + DConfig.DTV_BEFORE_BLK >= tm.getBlockRange.getEndBlock
        && vq.getBlockRange.getStartBlock >= tm.getBlockRange.getEndBlock
        && vq.getBlockRange.getStartBlock >= cn.getCurBlock
        && vq.getTermId > 0
        || (StringUtils.isNotBlank(vq.getLastTermUid) && vq.getLastTermId.equals(tm.getTermId))) {
        checkVoteDB(vq)
      } else if (cn.getCurBlock + DConfig.DTV_BEFORE_BLK >= tm.getBlockRange.getEndBlock
        || JodaTimeHelper.secondIntFromNow(tm.getTermEndMs) > DConfig.DTV_TIMEOUT_SEC) {

        val msgid = UUIDGenerator.generate();
        MDCSetMessageID(msgid);

        //      log.debug("try vote new term:");
        DCtrl.coMinerByUID.filter(p => {
          network.nodeByBcuid(p._1) == network.noneNode
        }).map { p =>
          log.debug("remove Node:" + p._1);
          DCtrl.coMinerByUID.remove(p._1);
        }

        val newterm = PSDutyTermVote.newBuilder();
        val conodescount = Math.min(DCtrl.coMinerByUID.size, DConfig.DTV_MAX_SUPER_MINER);
        val mineBlockCount = DConfig.DTV_MUL_BLOCKS_EACH_TERM * conodescount;
        val startBlk = cn.getCurBlock + 1;
        newterm.setBlockRange(BlockRange.newBuilder()
          .setStartBlock(startBlk)
          .setEndBlock(startBlk + mineBlockCount - 1)
          .setEachBlockSec(DConfig.BLK_EPOCH_SEC))
          .setCoNodes(conodescount)
          .setMessageId(msgid)
          .setCoAddress(DCtrl.instance.cur_dnode.getCoAddress)
          .setCwsGuaranty(DConfig.MAX_CWS_GUARANTY)
          .setSliceId(1)
          .setMaxTnxEachBlock(DConfig.MAX_TNX_EACH_BLOCK)
          .setTermStartMs(System.currentTimeMillis());

        newterm.setTermEndMs(DConfig.DTV_TIME_MS_EACH_BLOCK * mineBlockCount);

        newterm.setTermId(tm.getTermId + 1)
          .setLastTermId(tm.getTermId)
          .setLastTermUid(tm.getMessageId)
          .setSign(msgid)

        val rand = Math.random() * 1000
        val rdns = scala.util.Random.shuffle(DCtrl.coMinerByUID);
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
        log.debug("mineQ=" + newterm.getMinerQueueList.foldLeft(",")((a, b) => a + "," + b.getBlockHeight + "=" + b.getMinerCoaddr))

        log.debug("get coMinerNodeCount=" + DCtrl.coMinerByUID.size + ",NetworkDNodecount=" + network.directNodeByBcuid.size);

        //checking health remove offline nodes.

        val curtime = System.currentTimeMillis()
        DCtrl.instance.vote_Request = newterm;
        log.debug("try to vote:newterm=" + newterm.getTermId + ",curterm=" + tm.getTermId
          + ",voteN=" + conodescount + ",sign=" + newterm.getSign)
        network.dwallMessage("DTVDOB", Left(DCtrl.voteRequest().build()), msgid);
        false
      } else {
        false
      }
    })
  }

}
