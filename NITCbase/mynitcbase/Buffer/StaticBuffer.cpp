#include "StaticBuffer.h"
#include<cstdio>
unsigned char StaticBuffer::blocks[BUFFER_CAPACITY][BLOCK_SIZE];
struct BufferMetaInfo StaticBuffer::metainfo[BUFFER_CAPACITY];
unsigned char StaticBuffer::blockAllocMap[DISK_BLOCKS];

StaticBuffer::StaticBuffer() {
  for(int i=0;i<4;i++){
    Disk::readBlock(blockAllocMap+i*(BLOCK_SIZE),i);
  }
  // initialise all blocks as free
  for (int bufferIndex=0;bufferIndex<BUFFER_CAPACITY;bufferIndex++) {
    metainfo[bufferIndex].free = true;
    metainfo[bufferIndex].dirty = false;
    metainfo[bufferIndex].timeStamp = -1;
    metainfo[bufferIndex].blockNum = -1;
  }
}

int StaticBuffer::getStaticBlockType(int blockNum){
    // Check if blockNum is valid (non zero and less than number of disk blocks)
    // and return E_OUTOFBOUND if not valid.
    if(blockNum<0 || blockNum >= DISK_BLOCKS)
      return E_OUTOFBOUND;

    // Access the entry in block allocation map corresponding to the blockNum argument
    // and return the block type after type casting to integer.
    int blockType = (int)blockAllocMap[blockNum];
    return blockType;
}

int StaticBuffer::getFreeBuffer(int blockNum) {
  if (blockNum < 0 || blockNum > DISK_BLOCKS) {
    return E_OUTOFBOUND;
  }
  
  for(int i=0;i<BUFFER_CAPACITY;i++)
  {
     if(metainfo[i].free == false)
     {
        metainfo[i].timeStamp++;
     }
  }
  
  int allocatedBuffer = -1;
  for (int bufferIndex=0;bufferIndex<BUFFER_CAPACITY;bufferIndex++){
     if(metainfo[bufferIndex].free == true){
        allocatedBuffer = bufferIndex;
        break;
     }
  }
  int index;
  if(allocatedBuffer == -1)
  {
     int maxi = -1e9;
     for(int i=0;i<BUFFER_CAPACITY;i++)
     {
        if(metainfo[i].timeStamp>maxi)
        {
           maxi = metainfo[i].timeStamp;
           index = i;
        }
     }
     if(metainfo[index].dirty == true)
     {
        Disk::writeBlock(blocks[index],metainfo[index].blockNum);
     }
     allocatedBuffer = index;
  }

  metainfo[allocatedBuffer].free = false;
  metainfo[allocatedBuffer].blockNum = blockNum;
  metainfo[allocatedBuffer].dirty = false;
  metainfo[allocatedBuffer].timeStamp = 0;

  return allocatedBuffer;
}

/*int StaticBuffer::getFreeBuffer(int blockNum) {
  if (blockNum < 0 || blockNum > DISK_BLOCKS) {
    return E_OUTOFBOUND;
  }
  int allocatedBuffer;

  // iterate through all the blocks in the StaticBuffer
  // find the first free block in the buffer (check metainfo)
  // assign allocatedBuffer = index of the free block
  for (int bufferIndex=0;bufferIndex<32;bufferIndex++){
     if(metainfo[bufferIndex].free == true){
        allocatedBuffer = bufferIndex;
        break;
     }
  }

  metainfo[allocatedBuffer].free = false;
  metainfo[allocatedBuffer].blockNum = blockNum;

  return allocatedBuffer;
}*/

int StaticBuffer::getBufferNum(int blockNum) {
  // Check if blockNum is valid (between zero and DISK_BLOCKS)
  // and return E_OUTOFBOUND if not valid.
  if (blockNum < 0 || blockNum > DISK_BLOCKS) {
    return E_OUTOFBOUND;
  }

  // find and return the bufferIndex which corresponds to blockNum (check metainfo)
  for (int bufferIndex=0;bufferIndex<BUFFER_CAPACITY;bufferIndex++){
     if(metainfo[bufferIndex].blockNum == blockNum){
        return bufferIndex;
     }
  }
  // if block is not in the buffer
  return E_BLOCKNOTINBUFFER;
}

int StaticBuffer::setDirtyBit(int blockNum){
    // find the buffer index corresponding to the block using getBufferNum().
    int bufferNum = getBufferNum(blockNum);
    // if block is not present in the buffer (bufferNum = E_BLOCKNOTINBUFFER)
    //     return E_BLOCKNOTINBUFFER
    if(bufferNum == E_BLOCKNOTINBUFFER)
       return E_BLOCKNOTINBUFFER;
    // if blockNum is out of bound (bufferNum = E_OUTOFBOUND)
    //     return E_OUTOFBOUND
    else if(bufferNum == E_OUTOFBOUND)
        return E_OUTOFBOUND;
    // else
    //     (the bufferNum is valid)
    //     set the dirty bit of that buffer to true in metainfo
    else
    {
       metainfo[bufferNum].dirty = true;
    }
    return SUCCESS;
}

StaticBuffer::~StaticBuffer() {
  for(int i=0;i<4;i++)
  {
     Disk::writeBlock(blockAllocMap+i*(BLOCK_SIZE),i);
  }
  for (int bufferIndex=0;bufferIndex<BUFFER_CAPACITY;bufferIndex++)
  {
      if(metainfo[bufferIndex].free == false && metainfo[bufferIndex].dirty == true)
      {
          Disk::writeBlock(blocks[bufferIndex],metainfo[bufferIndex].blockNum);
      }
  }
}

