package org.brewchain.dposblk.test

import org.brewchain.dposblk.pbgens.Dposblock.PBlockEntry
import org.apache.commons.codec.binary.Base64
import org.brewchain.evmapi.gens.Block.BlockEntity

object TestBLKLoad {
  def main(args: Array[String]): Unit = {
    val strbase="ClQKIEc29WAR6c5UM/14JHk9nzWjCUe972OfVhSxpgBEjK/lIK+Yzui4LCjUHDIEAAAABkogCHgwIvtWaEYQNjOqBhezTNuT4dLX4eluDREPga6ZQzsSABpGCgRkcG9zGAYiHURQQlFwcTd0d1dacFdNVnVxZndXTnVtbnlxTWZaKh1EUEJRcHE3dHdXWnBXTVZ1cWZ3V051bW55cU1mWg=="
    val bb = Base64.decodeBase64(strbase)
    val blk = BlockEntity.newBuilder().mergeFrom(bb).build()
    
    println(blk.getHeader)
    
  }
}