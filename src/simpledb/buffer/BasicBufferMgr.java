package simpledb.buffer;

import simpledb.file.*;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Objects;

/**
 * Manages the pinning and unpinning of buffers to blocks.
 * @author Edward Sciore
 *
 */
class BasicBufferMgr {
   private Buffer[] bufferpool;
   private LinkedList<Integer> freeBuffers;
   private HashMap<Integer, Integer> blockMap;
   private int numAvailable;
   private int policy;
   private int clockArmPositon = 0;

   /**
    * Creates a buffer manager having the specified number
    * of buffer slots.
    * This constructor depends on both the {@link FileMgr} and
    * {@link simpledb.log.LogMgr LogMgr} objects
    * that it gets from the class
    * {@link simpledb.server.SimpleDB}.
    * Those objects are created during system initialization.
    * Thus this constructor cannot be called until
    * {@link simpledb.server.SimpleDB#initFileAndLogMgr(String)} or
    * is called first.
    * @param numbuffs the number of buffer slots to allocate
    */
   BasicBufferMgr(int numbuffs, int policy){
       this.policy = policy;
      bufferpool = new Buffer[numbuffs];
      numAvailable = numbuffs;
      freeBuffers = new LinkedList<>();
      blockMap = new HashMap<>();
      for (int i=0; i<numbuffs; i++){
          Buffer buffer = new Buffer();
          buffer.setLocationInPool(i);
          bufferpool[i] = buffer;
          freeBuffers.add(i);
      }
   }

   /**
    * Flushes the dirty buffers modified by the specified transaction.
    * @param txnum the transaction's id number
    */
   synchronized void flushAll(int txnum) {
      for (Buffer buff : bufferpool)
         if (buff.isModifiedBy(txnum))
         buff.flush();
   }

   /**
    * Pins a buffer to the specified block.
    * If there is already a buffer assigned to that block
    * then that buffer is used;
    * otherwise, an unpinned buffer from the pool is chosen.
    * Returns a null value if there are no available buffers.
    * @param blk a reference to a disk block
    * @return the pinned buffer
    */
   synchronized Buffer pin(Block blk) {
      Buffer buff = findExistingBuffer(blk);
      if (buff == null) {
         buff = chooseUnpinnedBuffer();
         if (buff == null)
            return null;
         buff.assignToBlock(blk);
          if(blockMap.containsValue(buff.getLocationInPool())){
              int key = getKeyByValue(blockMap, buff.getLocationInPool());
              blockMap.remove(key);
          }
         blockMap.put(buff.block().hashCode(), buff.getLocationInPool());
      }else{
          buff.setSecondChanceBit(1);
          buff.setTimestamp(System.currentTimeMillis());
      }
      if (!buff.isPinned())
         numAvailable--;
      buff.pin();
      return buff;
   }

   /**
    * Allocates a new block in the specified file, and
    * pins a buffer to it.
    * Returns null (without allocating the block) if
    * there are no available buffers.
    * @param filename the name of the file
    * @param fmtr a pageformatter object, used to format the new block
    * @return the pinned buffer
    */
   synchronized Buffer pinNew(String filename, PageFormatter fmtr) {
      Buffer buff = chooseUnpinnedBuffer();
      if (buff == null){
          return null;
      }else{
          buff.setSecondChanceBit(1);
          buff.setTimestamp(System.currentTimeMillis());
      }
      buff.assignToNew(filename, fmtr);
      numAvailable--;
      buff.pin();

      if(blockMap.containsValue(buff.getLocationInPool())){
          int key = getKeyByValue(blockMap, buff.getLocationInPool());
          blockMap.remove(key);
      }
       blockMap.put(buff.block().hashCode(), buff.getLocationInPool());
      return buff;
   }

   /**
    * Unpins the specified buffer.
    * @param buff the buffer to be unpinned
    */
   synchronized void unpin(Buffer buff) {
      buff.unpin();
      if (!buff.isPinned())
         numAvailable++;
   }

   /**
    * Returns the number of available (i.e. unpinned) buffers.
    * @return the number of available buffers
    */
   int available() {
      return numAvailable;
   }

   private Buffer findExistingBuffer(Block blk) {
      Integer location = blockMap.get(blk.hashCode());

      if(location!=null){
          return bufferpool[location];
      }else{
          return null;
      }
   }

   private Buffer chooseUnpinnedBuffer() {
       if(!freeBuffers.isEmpty()){
           Buffer buffer = bufferpool[freeBuffers.get(0)];
           freeBuffers.remove(0);
           return buffer;
       }
       switch (policy){
           case 0:
               return runReplaceUnpinned();
           case 1:
               return runLru();
           case 2:
               return runClockReplacement();
           default:
               System.out.println("ERROR INVALID POLICY INPUT!!");
               return null;
       }
   }

    public static <T, E> T getKeyByValue(Map<T, E> map, E value) {
        for (Map.Entry<T, E> entry : map.entrySet()) {
            if (Objects.equals(value, entry.getValue())) {
                return entry.getKey();
            }
        }
        return null;
    }

   /*public String toString(){
       int MAX = 8;
       boolean firstRun = true;
       String result = "";
       for(int i = 0; i < bufferpool.length; i++){
           if((i+1)%MAX == 0 && !firstRun){
               result += "\n";
           }
           if(bufferpool[i].block()!=null){
               result+=String.valueOf(bufferpool[i].block());
           }
       }
   }*/

   Buffer runReplaceUnpinned(){
       for(int i = 0; i < bufferpool.length; i++){
           if(!bufferpool[i].isPinned()){
               blockMap.remove(bufferpool[i].block().hashCode());
               return bufferpool[i];
           }
       }
       return null;
   }
   Buffer runLru(){
       long temp = Long.MAX_VALUE;
       int result = -1;
       for(int i = 0; i < bufferpool.length; i++){
           if(!bufferpool[i].isPinned() && (temp > bufferpool[i].getTimestamp())){
               temp = bufferpool[i].getTimestamp();
               result = i;
           }
       }
       blockMap.remove(bufferpool[result].block().hashCode());
       return bufferpool[result];
   }

   Buffer runClockReplacement(){
       while(clockArmPositon < bufferpool.length + 1){
           if(clockArmPositon == bufferpool.length){
                clockArmPositon = 0;
           }
           int i = clockArmPositon;
           if(!bufferpool[i].isPinned()){
               if(bufferpool[i].getSecondChanceBit() == 1){
                   bufferpool[i].setSecondChanceBit(0);
                   clockArmPositon++;
               }else{
                   blockMap.remove(bufferpool[i].block().hashCode());
                   clockArmPositon++;
                   return bufferpool[i];
               }
           }else{
               clockArmPositon++;
           }
       }
       return null;
   }
}
