package org.brewchain.dposblk.utils

import onight.tfw.mservice.NodeHelper
import onight.tfw.outils.conf.PropHelper

object DConfig {
  val prop: PropHelper = new PropHelper(null);
  val PROP_DOMAIN = "org.bc.dpos."

  val BLK_EPOCH_SEC = prop.get(PROP_DOMAIN + "blk.epoch.sec", 10); //2 seconds each block 

  val SYNCBLK_PAGE_SIZE = prop.get(PROP_DOMAIN + "syncblk.page.size", 10);

  val VOTE_QUORUM_RATIO = prop.get(PROP_DOMAIN + "vote.quorum.ratio", 60); //60%

  val SYNCBLK_MAX_RUNNER = prop.get(PROP_DOMAIN + "syncblk.max.runner", 10);
  val SYNCBLK_WAITSEC_NEXTRUN = prop.get(PROP_DOMAIN + "syncblk.waitsec.nextrun", 10);
  val SYNCBLK_WAITSEC_ALLRUN = prop.get(PROP_DOMAIN + "syncblk.waitsec.allrun", 600);

  val MAX_BLK_COUNT_PERTERM = prop.get(PROP_DOMAIN + "max.blk.count.perterm", 60);
  val MIN_BLK_COUNT_PERTERM = prop.get(PROP_DOMAIN + "min.blk.count.perterm", 30);

  val MAX_VOTE_WAIT_SEC = prop.get(PROP_DOMAIN + "max.vote.wait.sec", 10);

  val TICK_BLKCTRL_SEC = prop.get(PROP_DOMAIN + "tick.blkctrl.sec", 1);
  val INITDELAY_BLKCTRL_SEC = prop.get(PROP_DOMAIN + "initdelay.blkctrl.sec", 1);

  val DTV_BEFORE_BLK = prop.get(PROP_DOMAIN + "dtv.before.blk", 5);

  val DTV_TIMEOUT_SEC = prop.get(PROP_DOMAIN + "dtv.timeout.sec", -10);
  
  val DTV_MUL_BLOCKS_EACH_TERM = prop.get(PROP_DOMAIN + "dtv.mul.blocks.each.term", 2);
  val DTV_MAX_SUPER_MINER = prop.get(PROP_DOMAIN + "dtv.max.super.miner", 31);
  val DTV_MIN_SUPER_MINER = prop.get(PROP_DOMAIN + "dtv.min.super.miner", 5);
  val DTV_TIME_MS_EACH_BLOCK = prop.get(PROP_DOMAIN + "dtv.time.ms.each_block", 1000);

}