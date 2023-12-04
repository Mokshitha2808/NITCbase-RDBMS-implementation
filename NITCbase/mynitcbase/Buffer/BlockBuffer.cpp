#include "BlockBuffer.h"
#include <cstdlib>
#include <cstring>

BlockBuffer::BlockBuffer(int blockNum) {
  this->blockNum=blockNum;  //me
}

BlockBuffer::BlockBuffer(char blockType){
    // allocate a block on the disk and a buffer in memory to hold the new block of
    // given type using getFreeBlock function and get the return error codes if any.
  int type;
  if(blockType == 'R')
  {
     type = REC;
  }
  else if(blockType == 'L')
  {
     type = IND_LEAF;
  }
  else if(blockType == 'I')
  {
     type = IND_INTERNAL;
  }
  else
  {
     type = UNUSED_BLK;
  }
  int blockNum=getFreeBlock(type);
  this->blockNum=blockNum;
  //if(blockNum==E_CACHEFULL)
   // set the blockNum field of the object to that of the allocated block
    // number if the method returned a valid block number,
    // otherwise set the error code returned as the block number.

    // (The caller must check if the constructor allocatted block successfully
    // by checking the value of block number field.)
}

// calls the parent class constructor
RecBuffer::RecBuffer(int blockNum) : BlockBuffer::BlockBuffer(blockNum) {}

RecBuffer::RecBuffer() : BlockBuffer('R'){}
//call parent non-default constructor with blockNum

// call the corresponding parent constructor
IndBuffer::IndBuffer(char blockType) : BlockBuffer(blockType){}

IndBuffer::IndBuffer(int blockNum) : BlockBuffer(blockNum){}

IndInternal::IndInternal() : IndBuffer('I'){}
// call the corresponding parent constructor
// 'I' used to denote IndInternal.

IndInternal::IndInternal(int blockNum) : IndBuffer(blockNum){}
// call the corresponding parent constructor

IndLeaf::IndLeaf() : IndBuffer('L'){} // this is the way to call parent non-default constructor.
                      // 'L' used to denote IndLeaf.

//this is the way to call parent non-default constructor.
IndLeaf::IndLeaf(int blockNum) : IndBuffer(blockNum){}

int BlockBuffer::getBlockNum(){
	return this->blockNum;
    //return corresponding block number.
}

// load the block header into the argument pointer
int BlockBuffer::getHeader(struct HeadInfo *head) {
  
  unsigned char *bufferPtr;
  int ret = loadBlockAndGetBufferPtr(&bufferPtr);
  if (ret != SUCCESS) {
    return ret;   // return any errors that might have occured in the process
  }  
  struct HeadInfo *bufferHeader = (struct HeadInfo *)bufferPtr;
  head->blockType = bufferHeader->blockType;
  head->numSlots = bufferHeader->numSlots;
  head->numEntries = bufferHeader->numEntries;
  head->numAttrs = bufferHeader->numAttrs;
  head->rblock = bufferHeader->rblock;
  head->lblock = bufferHeader->lblock;
  head->pblock = bufferHeader->pblock;
   
  /*memcpy(&head->numSlots, bufferPtr + 24, 4);
  memcpy(&head->numEntries,bufferPtr+16, 4);
  memcpy(&head->numAttrs,bufferPtr+20, 4);
  memcpy(&head->rblock,bufferPtr+12, 4);
  memcpy(&head->lblock,bufferPtr+8, 4);*/
  return SUCCESS;
  
}

// load the record at slotNum into the argument pointer
int RecBuffer::getRecord(union Attribute *rec, int slotNum) {
  struct HeadInfo head;
  
  this->getHeader(&head);

  int attrCount = head.numAttrs;
  int slotCount = head.numSlots;
  
  unsigned char buffer[BLOCK_SIZE];
  unsigned char *bufferPtr;
  int ret = loadBlockAndGetBufferPtr(&bufferPtr);
  if (ret != SUCCESS) {
    return ret;
  }
  
  int recordSize = attrCount * ATTR_SIZE;
  int offset = 32 + slotCount + (recordSize * slotNum);
  unsigned char *slotPointer = bufferPtr + offset;
  memcpy(rec, slotPointer, recordSize);

  return SUCCESS;
}

int RecBuffer::setRecord(union Attribute *rec, int slotNum) {
    unsigned char *bufferPtr;
    /* get the starting address of the buffer containing the block
       using loadBlockAndGetBufferPtr(&bufferPtr). */
      int res = loadBlockAndGetBufferPtr(&bufferPtr);
      if(res != SUCCESS)
         return res;
    // if loadBlockAndGetBufferPtr(&bufferPtr) != SUCCESS
        // return the value returned by the call.

    /* get the header of the block using the getHeader() function */
     HeadInfo head;
     this->getHeader(&head);
    // get number of attributes in the block.
    int numAttrs = head.numAttrs;
    // get the number of slots in the block.
    int numSlots = head.numSlots;
    // if input slotNum is not in the permitted range return E_OUTOFBOUND.
    if(slotNum >= numSlots || slotNum<0)
       return E_OUTOFBOUND;
    /* offset bufferPtr to point to the beginning of the record at required
       slot. the block contains the header, the slotmap, followed by all
       the records. so, for example,
       record at slot x will be at bufferPtr + HEADER_SIZE + (x*recordSize)
       copy the record from `rec` to buffer using memcpy
       (hint: a record will be of size ATTR_SIZE * numAttrs)
    */
    int recordSize = numAttrs * ATTR_SIZE;
    int offset =  numSlots + 32 + (recordSize * slotNum);
    unsigned char *slotPointer = bufferPtr + offset;
    memcpy(slotPointer,rec,recordSize);
    // update dirty bit using setDirtyBit()
    return StaticBuffer::setDirtyBit(this->blockNum);
    /* (the above function call should not fail since the block is already
       in buffer and the blockNum is valid. If the call does fail, there
       exists some other issue in the code) */
}

int BlockBuffer::loadBlockAndGetBufferPtr(unsigned char ** buffPtr) {
    // check whether the block is already present in the buffer
       //using StaticBuffer.getBufferNum() 
    int bufferNum = StaticBuffer::getBufferNum(this->blockNum);

    // if present (!=E_BLOCKNOTINBUFFER),
        // set the timestamp of the corresponding buffer to 0 and increment the
        // timestamps of all other occupied buffers in BufferMetaInfo.
    if(bufferNum != E_BLOCKNOTINBUFFER)
    {
       for(int i=0;i<32;i++)
       {
          if(StaticBuffer::metainfo[i].free == false)
             StaticBuffer::metainfo[i].timeStamp++;
       }
       StaticBuffer::metainfo[bufferNum].timeStamp = 0;
    }

    // else
        // get a free buffer using StaticBuffer.getFreeBuffer()

        // if the call returns E_OUTOFBOUND, return E_OUTOFBOUND here as
        // the blockNum is invalid

        // Read the block into the free buffer using readBlock()
    else
    {
        bufferNum = StaticBuffer::getFreeBuffer(this->blockNum);
        if(bufferNum == E_OUTOFBOUND)
          return E_OUTOFBOUND;
        Disk::readBlock(StaticBuffer::blocks[bufferNum], this->blockNum);
    }
    // store the pointer to this buffer (blocks[bufferNum]) in *buffPtr
    *buffPtr = StaticBuffer::blocks[bufferNum];
    return SUCCESS;
    // return SUCCESS;
}

int RecBuffer::getSlotMap(unsigned char *slotMap) {
  unsigned char *bufferPtr;

  // get the starting address of the buffer containing the block using loadBlockAndGetBufferPtr().
  int ret = loadBlockAndGetBufferPtr(&bufferPtr);
  if (ret != SUCCESS) {
    return ret;
  }

  struct HeadInfo head;
  // get the header of the block using getHeader() function
  this->getHeader(&head);

  int slotCount = head.numSlots;
  //printf("%d\n",slotCount);
  // get a pointer to the beginning of the slotmap in memory by offsetting HEADER_SIZE
  unsigned char *slotMapInBuffer = bufferPtr + HEADER_SIZE;

  // copy the values from `slotMapInBuffer` to `slotMap` (size is `slotCount`)
 
  
  memcpy(slotMap,slotMapInBuffer,slotCount);
  

  return SUCCESS;
}


int RecBuffer::setSlotMap(unsigned char *slotMap) {
    unsigned char *bufferPtr;
    /* get the starting address of the buffer containing the block using
       loadBlockAndGetBufferPtr(&bufferPtr). */
     int ret = loadBlockAndGetBufferPtr(&bufferPtr);
    // if loadBlockAndGetBufferPtr(&bufferPtr) != SUCCESS
        // return the value returned by the call.
     if(ret != SUCCESS)
        return ret;
    // get the header of the block using the getHeader() function
    struct HeadInfo head;
    this->getHeader(&head);
    int numSlots = head.numSlots;

    // the slotmap starts at bufferPtr + HEADER_SIZE. Copy the contents of the
    // argument `slotMap` to the buffer replacing the existing slotmap.
    // Note that size of slotmap is `numSlots`
    unsigned char *slotMapInBuffer = bufferPtr + HEADER_SIZE;
    memcpy(slotMapInBuffer,slotMap,numSlots);
    // update dirty bit using StaticBuffer::setDirtyBit
    // if setDirtyBit failed, return the value returned by the call
    return StaticBuffer::setDirtyBit(this->blockNum);
    // return SUCCESS
}


int BlockBuffer::setHeader(struct HeadInfo *head){

    unsigned char *bufferPtr;
    // get the starting address of the buffer containing the block using
    // loadBlockAndGetBufferPtr(&bufferPtr).
    int ret=loadBlockAndGetBufferPtr(&bufferPtr);

    // if loadBlockAndGetBufferPtr(&bufferPtr) != SUCCESS
        // return the value returned by the call.
	if(ret!=SUCCESS){
		return ret;
	}
    // cast bufferPtr to type HeadInfo*
    struct HeadInfo *bufferHeader = (struct HeadInfo *)bufferPtr;

    // copy the fields of the HeadInfo pointed to by head (except reserved) to
    // the header of the block (pointed to by bufferHeader)
    //(hint: bufferHeader->numSlots = head->numSlots )
    
    bufferHeader->blockType=head->blockType;
    bufferHeader->pblock=head->pblock;
    bufferHeader->lblock=head->lblock;
    bufferHeader->rblock=head->rblock;
    bufferHeader->numEntries=head->numEntries;
    bufferHeader->numAttrs=head->numAttrs;
    bufferHeader->numSlots=head->numSlots;
	
    // update dirty bit by calling StaticBuffer::setDirtyBit()
    // if setDirtyBit() failed, return the error code
    int rett=StaticBuffer::setDirtyBit(this->blockNum);
    
    if(rett!=SUCCESS){
    	return rett;
    }

    // return SUCCESS;
    return SUCCESS;
}


int BlockBuffer::setBlockType(int blockType){

    unsigned char *bufferPtr;
    /* get the starting address of the buffer containing the block
       using loadBlockAndGetBufferPtr(&bufferPtr). */
    int ret = loadBlockAndGetBufferPtr(&bufferPtr);
    // if loadBlockAndGetBufferPtr(&bufferPtr) != SUCCESS
        // return the value returned by the call.
    if(ret != SUCCESS)
        return ret;
       
    // store the input block type in the first 4 bytes of the buffer.
    // (hint: cast bufferPtr to int32_t* and then assign it)
   *((int32_t *)bufferPtr) = blockType;
    //object's block number to `blockType`.
    /*
   if(blockType==(int)'R')
       StaticBuffer::blockAllocMap[this->blockNum]=REC;
   if(blockType==(int)'I')
      StaticBuffer::blockAllocMap[this->blockNum]=IND_INTERNAL;
   if(blockType==(int)'L')
      StaticBuffer::blockAllocMap[this->blockNum]=IND_LEAF;
      */
    // update dirty bit by calling 
   StaticBuffer::blockAllocMap[this->blockNum]=blockType;
   return StaticBuffer::setDirtyBit(this->blockNum);
     //*((int32_t *)bufferPtr) = blockType;
    // update the StaticBuffer::blockAllocMap entry corresponding to the
    // object's block number to `blockType`.
    //StaticBuffer::blockAllocMap[this->blockNum] = blockType;
    // update dirty bit by calling StaticBuffer::setDirtyBit()
    // if setDirtyBit() failed
        // return the returned value from the call
    //return StaticBuffer::setDirtyBit(this->blockNum);
    // return SUCCESS
}

int BlockBuffer::getFreeBlock(int blockType){

    // iterate through the StaticBuffer::blockAllocMap and find the block number
    // of a free block in the disk.
    int blocknum = -1;
    for(int i=0;i<DISK_BLOCKS;i++)
    {
       if(StaticBuffer::blockAllocMap[i]==UNUSED_BLK){
            blocknum = i;
            break;
       }
    }
    if(blocknum == -1)
      return E_DISKFULL;
    // if no block is free, return E_DISKFULL.

    // set the object's blockNum to the block number of the free block.
    StaticBuffer::getFreeBuffer(blocknum);
    
     this->blockNum=blocknum;
     struct HeadInfo head;
     head.pblock=-1;
     head.lblock=-1;
     head.rblock=-1;
     head.numEntries=0;
     head.numAttrs=0;
     head.numSlots=0;
     this->setHeader(&head);
     setBlockType(blockType);
     return this->blockNum;
    // find a free buffer using StaticBuffer::getFreeBuffer() .

    // initialize the header of the block passing a struct HeadInfo with values
    // pblock: -1, lblock: -1, rblock: -1, numEntries: 0, numAttrs: 0, numSlots: 0
    // to the setHeader() function.

    // update the block type of the block to the input block type using setBlockType().

    // return block number of the free block.
}

void BlockBuffer::releaseBlock(){

    // if blockNum is INVALID_BLOCK (-1), or it is invalidated already, do nothing
    if(this->blockNum == E_INVALIDBLOCK)
       return;
    // else
    else
    {
        /* get the buffer number of the buffer assigned to the block
           using StaticBuffer::getBufferNum().
           (this function return E_BLOCKNOTINBUFFER if the block is not
           currently loaded in the buffer)
            */
        int ret = StaticBuffer::getBufferNum(this->blockNum);
        if(ret == E_BLOCKNOTINBUFFER)
          return;
        // if the block is present in the buffer, free the buffer
        // by setting the free flag of its StaticBuffer::tableMetaInfo entry
        // to true.
        StaticBuffer::metainfo[ret].free = true;
        // free the block in disk by setting the data type of the entry
        // corresponding to the block number in StaticBuffer::blockAllocMap
        // to UNUSED_BLK.
        StaticBuffer::blockAllocMap[this->blockNum] = UNUSED_BLK;
        // set the object's blockNum to INVALID_BLOCK (-1)
        this->blockNum = E_INVALIDBLOCK;
     }
}

int IndInternal::getEntry(void *ptr, int indexNum) {
    // if the indexNum is not in the valid range of [0, MAX_KEYS_INTERNAL-1]
    //     return E_OUTOFBOUND.
    if(indexNum<0 || indexNum>=MAX_KEYS_INTERNAL)
      return E_OUTOFBOUND;

    unsigned char *bufferPtr;
    /* get the starting address of the buffer containing the block
       using loadBlockAndGetBufferPtr(&bufferPtr). */
    int ret = loadBlockAndGetBufferPtr(&bufferPtr);
    // if loadBlockAndGetBufferPtr(&bufferPtr) != SUCCESS
    //     return the value returned by the call.
    if(ret != SUCCESS)
      return ret;
    // typecast the void pointer to an internal entry pointer
    struct InternalEntry *internalEntry = (struct InternalEntry *)ptr;

    /*
    - copy the entries from the indexNum`th entry to *internalEntry
    - make sure that each field is copied individually as in the following code
    - the lChild and rChild fields of InternalEntry are of type int32_t
    - int32_t is a type of int that is guaranteed to be 4 bytes across every
      C++ implementation. sizeof(int32_t) = 4
    */
    

    /* the indexNum'th entry will begin at an offset of
       HEADER_SIZE + (indexNum * (sizeof(int) + ATTR_SIZE) )         [why?]
       from bufferPtr */
    unsigned char *entryPtr = bufferPtr + HEADER_SIZE + (indexNum * 20);

    memcpy(&(internalEntry->lChild), entryPtr, sizeof(int32_t));
    memcpy(&(internalEntry->attrVal), entryPtr + 4, sizeof(Attribute));
    memcpy(&(internalEntry->rChild), entryPtr + 20, 4);

    return SUCCESS;
}

int IndLeaf::getEntry(void *ptr, int indexNum) {

    // if the indexNum is not in the valid range of [0, MAX_KEYS_LEAF-1]
    //     return E_OUTOFBOUND.
    if(indexNum<0 || indexNum>=MAX_KEYS_LEAF)
      return E_OUTOFBOUND;

    unsigned char *bufferPtr;
    int ret = loadBlockAndGetBufferPtr(&bufferPtr);

    if(ret != SUCCESS)
      return ret;
    // copy the indexNum'th Index entry in buffer to memory ptr using memcpy

    /* the indexNum'th entry will begin at an offset of
       HEADER_SIZE + (indexNum * LEAF_ENTRY_SIZE)  from bufferPtr */
    unsigned char *entryPtr = bufferPtr + HEADER_SIZE + (indexNum * LEAF_ENTRY_SIZE);
    memcpy((struct Index *)ptr, entryPtr, LEAF_ENTRY_SIZE);

    return SUCCESS;
}


/*int IndInternal::setEntry(void *ptr, int indexNum) {
  return 0;
}*/

/*int IndLeaf::setEntry(void *ptr, int indexNum) {
  return 0;
}*/
int IndInternal::setEntry(void *ptr, int indexNum) {
    // if the indexNum is not in the valid range of [0, MAX_KEYS_INTERNAL-1]
    //     return E_OUTOFBOUND.
    if(indexNum < 0 || indexNum >= MAX_KEYS_INTERNAL)
      return E_OUTOFBOUND;

    unsigned char *bufferPtr;
    /* get the starting address of the buffer containing the block
       using loadBlockAndGetBufferPtr(&bufferPtr). */
    int ret = loadBlockAndGetBufferPtr(&bufferPtr);
    // if loadBlockAndGetBufferPtr(&bufferPtr) != SUCCESS
    //     return the value returned by the call.
    if(ret != SUCCESS)
       return ret;
    // typecast the void pointer to an internal entry pointer
    struct InternalEntry *internalEntry = (struct InternalEntry *)ptr;

    /*
    - copy the entries from *internalEntry to the indexNum`th entry
    - make sure that each field is copied individually as in the following code
    - the lChild and rChild fields of InternalEntry are of type int32_t
    - int32_t is a type of int that is guaranteed to be 4 bytes across every
      C++ implementation. sizeof(int32_t) = 4
    */

    /* the indexNum'th entry will begin at an offset of
       HEADER_SIZE + (indexNum * (sizeof(int) + ATTR_SIZE) )         [why?]
       from bufferPtr */

    unsigned char *entryPtr = bufferPtr + HEADER_SIZE + (indexNum * 20);

    memcpy(entryPtr, &(internalEntry->lChild), 4);
    memcpy(entryPtr + 4, &(internalEntry->attrVal), ATTR_SIZE);
    memcpy(entryPtr + 20, &(internalEntry->rChild), 4);

    return StaticBuffer::setDirtyBit(this->blockNum);
    // update dirty bit using setDirtyBit()
    // if setDirtyBit failed, return the value returned by the call

    // return SUCCESS
}

int IndLeaf::setEntry(void *ptr, int indexNum) {

    // if the indexNum is not in the valid range of [0, MAX_KEYS_LEAF-1]
    //     return E_OUTOFBOUND.
    if(indexNum < 0 || indexNum >= MAX_KEYS_LEAF)
      return E_OUTOFBOUND;

    unsigned char *bufferPtr;
    /* get the starting address of the buffer containing the block
       using loadBlockAndGetBufferPtr(&bufferPtr). */
    int ret = loadBlockAndGetBufferPtr(&bufferPtr);
    // if loadBlockAndGetBufferPtr(&bufferPtr) != SUCCESS
    //     return the value returned by the call.
    if(ret != SUCCESS)
       return ret;
    // copy the Index at ptr to indexNum'th entry in the buffer using memcpy

    /* the indexNum'th entry will begin at an offset of
       HEADER_SIZE + (indexNum * LEAF_ENTRY_SIZE)  from bufferPtr */
    unsigned char *entryPtr = bufferPtr + HEADER_SIZE + (indexNum * LEAF_ENTRY_SIZE);
    memcpy(entryPtr, (struct Index *)ptr, LEAF_ENTRY_SIZE);

    // update dirty bit using setDirtyBit()
    // if setDirtyBit failed, return the value returned by the call
    return StaticBuffer::setDirtyBit(this->blockNum);
    //return SUCCESS
}

int compareAttrs(union Attribute attr1, union Attribute attr2, int attrType) {
    //count++;
    double diff;
    if(attrType == STRING)
       diff = strcmp(attr1.sVal,attr2.sVal);
    else
      diff = attr1.nVal - attr2.nVal;
      
    if(diff > 0)
        return 1;
    if(diff < 0)
       return -1;
    return 0;
}

