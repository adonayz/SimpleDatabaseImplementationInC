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
   private LinkedList<Integer> freeBuffers; // CS4432-Project1: This list is used to find free frames in the buffer manager using O(1) time
   private HashMap<Integer, Integer> blockMap; // CS4432-Project1: This map is used to find the assigned frame# of a block in O(1) time
   private int numAvailable;
   private int policy; // CS4432-Project1: This variable is used to store the policy selected for replacement
   private int clockArmPositon = 0; // CS4432-Project1: This variable is used to track the last known location of the clock's arm

    // CS4432-Project1: Edited to accommodate new replacement policies and efficient disk block and free frame tracking methods
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
       this.policy = policy; // CS4432-Project1: stores policy
      bufferpool = new Buffer[numbuffs];
      numAvailable = numbuffs;
      freeBuffers = new LinkedList<>();// CS4432-Project1: initializes list
      blockMap = new HashMap<>(); // CS4432-Project1: initializes map
      for (int i=0; i<numbuffs; i++){
          Buffer buffer = new Buffer();
          buffer.setLocationInPool(i); // CS4432-Project1: saves the position of the buffer in the manager into
                                        // the buffer itself inorder to avoid scanning buffer array to find position in the future
          bufferpool[i] = buffer;
          freeBuffers.add(i);  // CS4432-Project1: tracks it as a free frame buffer
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

    // CS4432-Project1: Every time a buffer is pinned, the block in it is mapped to the frames position in a map inorder
    //                  to efficiently track the position of disk blocks in the buffer manager
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
       System.out.println(this.toString());
      Buffer buff = findExistingBuffer(blk);
      if (buff == null) {
         buff = chooseUnpinnedBuffer();
         if (buff == null)
            return null;
         buff.assignToBlock(blk);
          if(blockMap.containsValue(buff.getLocationInPool())){
              int key = getKeyByValue(blockMap, buff.getLocationInPool());  // CS4432-Project1: If the frame is already in use, remove
                                                                            // the current frame from the map before replacing it
              blockMap.remove(key);
          }
         blockMap.put(buff.block().hashCode(), buff.getLocationInPool());   // map new block to position
      }else{
          buff.setSecondChanceBit(1);   // // CS4432-Project1: reset second chance bit if is pinned again
          buff.setTimestamp(System.nanoTime());
      }
      if (!buff.isPinned())
         numAvailable--;
      buff.pin();
      System.out.println(this.toString());
      return buff;
   }

    /// CS4432-Project1: Every time a buffer is pinned, the block in it is mapped to the frames position in a map inorder
    //                  to efficiently track the position of disk blocks in the buffer manager
    // In function content similar to above
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
          buff.setTimestamp(System.nanoTime());
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

    // CS4432-Project1: Use the new mapping method implemented to find buffers.
    //                  It is much more efficient than the previous one. Time = O(1). No scanning required.
   private Buffer findExistingBuffer(Block blk) {
      Integer location = blockMap.get(blk.hashCode());

      if(location!=null){
          return bufferpool[location];
      }else{
          return null;
      }
   }

    // CS4432-Project1: Use replacement polices inorder to choose unpinned pages
   private Buffer chooseUnpinnedBuffer() {
       // CS4432-Project1: before replacing any page/buffer, check to see if there are free frames
       if(!freeBuffers.isEmpty()){
           Buffer buffer = bufferpool[freeBuffers.get(0)];
           freeBuffers.remove(0);
           return buffer;
       }
       // CS4432-Project1: if not choose a policy
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

    // CS4432-Project1: This is a helper function for my map. It uses value to get the key.
    // I used the following source as a reference https://stackoverflow.com/questions/1383797/java-hashmap-how-to-get-key-from-value
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

    // CS4432-Project1: Only replace if you find unpinned pages. This is the default policy
   Buffer runReplaceUnpinned(){
       for(int i = 0; i < bufferpool.length; i++){
           if(!bufferpool[i].isPinned()){
               blockMap.remove(bufferpool[i].block().hashCode());
               return bufferpool[i];
           }
       }
       return null;
   }

    // CS4432-Project1: Least replacement policy implementation
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

    // CS4432-Project1: Clock replacement policy implementation
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

    // CS4432-Project1: My reporting function for this class
    @Override
    public String toString(){
       String result = "";
       int rowSize = 10;
        for(int i = 0; i < bufferpool.length; i += rowSize)
        {
            for(int j = i; j < rowSize+i && j < bufferpool.length; j++)
            {
                result+= bufferpool[j].toString() + "\t";
            }
            result += "\n";
        }
        return result;
    }
}
