#include "BlockAccess.h"
#include<cstdio>
#include <cstring>

RecId BlockAccess::linearSearch(int relId, char attrName[ATTR_SIZE], union Attribute attrVal, int op) {
    // get the previous search index of the relation relId from the relation cache
    // (use RelCacheTable::getSearchIndex() function)
       RecId prevRecId;
       RelCacheTable::getSearchIndex(relId,&prevRecId);
       int block,slot;
    // let block and slot denote the record id of the record being currently checked

    // if the current search index record is invalid(i.e. both block and slot = -1)
    if (prevRecId.block == -1 && prevRecId.slot == -1)
    {
        // (no hits from previous search; search should start from the
        // first record itself)

        // get the first record block of the relation from the relation cache
        // (use RelCacheTable::getRelCatEntry() function of Cache Layer)
         RelCatEntry relcatentry;
         RelCacheTable::getRelCatEntry(relId,&relcatentry);
         
        // block = first record block of the relation
        // slot = 0
        block = relcatentry.firstBlk;
        slot = 0;
    }
    else
    {
        // (there is a hit from previous search; search should start from
        // the record next to the search index record)

        // block = search index's block
        // slot = search index's slot + 1
        block = prevRecId.block;
        slot = prevRecId.slot + 1;
    }

    /* The following code searches for the next record in the relation
       that satisfies the given condition
       We start from the record id (block, slot) and iterate over the remaining
       records of the relation
    */
    while (block != -1)
    {
        /* create a RecBuffer object for block (use RecBuffer Constructor for
           existing block) */
        RecBuffer recbuffer(block);
        // get the record with id (block, slot) using RecBuffer::getRecord()
        struct HeadInfo head;
        recbuffer.getHeader(&head);
        int attrs = head.numAttrs;
        int entries = head.numSlots;
        union Attribute record[attrs];
        recbuffer.getRecord(record,slot);
        unsigned char slotMap[entries];
        recbuffer.getSlotMap(slotMap);
        // get header of the block using RecBuffer::getHeader() function
        // get slot map of the block using RecBuffer::getSlotMap() function
        // If slot >= the number of slots per block(i.e. no more slots in this block)
        {
            // update block = right block of block
            // update slot = 0
            //continue;  // continue to the beginning of this while loop
        }
        if(slot >= entries)
        {
           block = head.rblock;
           slot = 0;
           continue;
        }
        // if slot is free skip the loop
        // (i.e. check if slot'th entry in slot map of block contains SLOT_UNOCCUPIED)
        if(slotMap[slot] == SLOT_UNOCCUPIED)
        {
            // increment slot and continue to the next record slot
            slot++;
            continue;
        }
        
        // compare record's attribute value to the the given attrVal as below:
        /*
            firstly get the attribute offset for the attrName attribute
            from the attribute cache entry of the relation using
            AttrCacheTable::getAttrCatEntry()
        */
         
         AttrCatEntry attrCatbuf;
         AttrCacheTable::getAttrCatEntry(relId,attrName,&attrCatbuf);
         int offset = attrCatbuf.offset;
         int attrType = attrCatbuf.attrType;
        /* use the attribute offset to get the value of the attribute from
           current record */
        
        union Attribute attrVal1 = record[offset];
        int cmpVal;  // will store the difference between the attributes
        // set cmpVal using compareAttrs()
        cmpVal = compareAttrs(attrVal1,attrVal,attrType);
        /* Next task is to check whether this record satisfies the given condition.
           It is determined based on the output of previous comparison and
           the op value received.
           The following code sets the cond variable if the condition is satisfied.
        */
        if (
            (op == NE && cmpVal != 0) ||    // if op is "not equal to"
            (op == LT && cmpVal < 0) ||     // if op is "less than"
            (op == LE && cmpVal <= 0) ||    // if op is "less than or equal to"
            (op == EQ && cmpVal == 0) ||    // if op is "equal to"
            (op == GT && cmpVal > 0) ||     // if op is "greater than"
            (op == GE && cmpVal >= 0)       // if op is "greater than or equal to"
        ) {
            /*                   
            set the search index in the relation cache as
            the record id of the record that satisfies the given condition
            (use RelCacheTable::setSearchIndex function)
            */
            RecId searchIndex;
            searchIndex.block = block;
            searchIndex.slot = slot;
            RelCacheTable::setSearchIndex(relId,&searchIndex);
            return RecId{block, slot};
        }

        slot++;
    }
    
    // no record in the relation with Id relid satisfies the given condition
    return RecId{-1, -1};
}

int BlockAccess::renameRelation(char oldName[ATTR_SIZE], char newName[ATTR_SIZE]){
    /* reset the searchIndex of the relation catalog using
       RelCacheTable::resetSearchIndex() */                      
    RelCacheTable::resetSearchIndex(0); 
    Attribute newRelationName;    // set newRelationName with newName
    strcpy(newRelationName.sVal,newName);
    // search the relation catalog for an entry with "RelName" = newRelationName
    char attrName[ATTR_SIZE];
    strcpy(attrName,"RelName");
    RecId recordId{-1,-1};
    recordId = BlockAccess::linearSearch(0,attrName,newRelationName,EQ);
    // If relation with name newName already exists (result of linearSearch
    //                                               is not {-1, -1})
    //    return E_RELEXIST;
    if(recordId.block!= -1 && recordId.slot!= -1)
       return E_RELEXIST;

    /* reset the searchIndex of the relation catalog using
       RelCacheTable::resetSearchIndex() */
    RelCacheTable::resetSearchIndex(0); 
    Attribute oldRelationName;    // set oldRelationName with oldName
    strcpy(oldRelationName.sVal,oldName);
    // search the relation catalog for an entry with "RelName" = oldRelationName
    recordId = BlockAccess::linearSearch(0,attrName,oldRelationName,EQ);
    // If relation with name oldName does not exist (result of linearSearch is {-1, -1})
    //    return E_RELNOTEXIST;
    if(recordId.block == -1 && recordId.slot == -1)
        return E_RELNOTEXIST;
    /* get the relation catalog record of the relation to rename using a RecBuffer
       on the relation catalog [RELCAT_BLOCK] and RecBuffer.getRecord function
    */
    RecBuffer recbuffer(recordId.block);
    union Attribute rec[6];
    recbuffer.getRecord(rec,recordId.slot);
    /* update the relation name attribute in the record with newName.
       (use RELCAT_REL_NAME_INDEX) */
    // set back the record value using RecBuffer.setRecord
    strcpy(rec[0].sVal,newName);
    recbuffer.setRecord(rec,recordId.slot);
    int numAttrs = rec[1].nVal;
    /*
    update all the attribute catalog entries in the attribute catalog corresponding
    to the relation with relation name oldName to the relation name newName
    */

    /* reset the searchIndex of the attribute catalog using
       RelCacheTable::resetSearchIndex() */
    RelCacheTable::resetSearchIndex(1);
    //for i = 0 to numberOfAttributes :
    //    linearSearch on the attribute catalog for relName = oldRelationName
    //    get the record using RecBuffer.getRecord
    //
    //    update the relName field in the record to newName
    //    set back the record using RecBuffer.setRecord
    RecId recid{-1,-1};
    union Attribute record[6];
    for(int i=0;i<numAttrs;i++)
    {
       recid = linearSearch(1,attrName,oldRelationName,EQ);
       if(recid.block!=-1 && recid.slot!=-1){
       RecBuffer attrcatblock(recid.block);
       attrcatblock.getRecord(record,recid.slot);
       strcpy(record[0].sVal,newName);
       attrcatblock.setRecord(record,recid.slot); 
       }                                   
    }
    return SUCCESS;
}

int BlockAccess::renameAttribute(char relName[ATTR_SIZE], char oldName[ATTR_SIZE], char newName[ATTR_SIZE]) {

    /* reset the searchIndex of the relation catalog using
       RelCacheTable::resetSearchIndex() */
    RelCacheTable::resetSearchIndex(0);
    Attribute relNameAttr;    // set relNameAttr to relName                 
    strcpy(relNameAttr.sVal,relName);
    // Search for the relation with name relName in relation catalog using linearSearch()
    // If relation with name relName does not exist (search returns {-1,-1})
    //    return E_RELNOTEXIST;
    char attrName[ATTR_SIZE];
    strcpy(attrName,"RelName");
    
    RecId recordId{-1,-1};
    recordId = BlockAccess::linearSearch(0,attrName,relNameAttr,EQ);
    RelCacheTable::resetSearchIndex(0);
    if(recordId.block == -1 && recordId.slot == -1)
       return E_RELNOTEXIST;
    /* reset the searchIndex of the attribute catalog using
       RelCacheTable::resetSearchIndex() */
    RelCacheTable::resetSearchIndex(1); 
    /* declare variable attrToRenameRecId used to store the attr-cat recId
    of the attribute to rename */
    RecId attrToRenameRecId{-1, -1};
    Attribute attrCatEntryRecord[ATTRCAT_NO_ATTRS];

    /* iterate over all Attribute Catalog Entry record corresponding to the
       relation to find the required attribute */
    union Attribute rec[6];
    while (true) {
        // linear search on the attribute catalog for RelName = relNameAttr
        recordId = BlockAccess::linearSearch(1,attrName,relNameAttr,EQ);
        // if there are no more attributes left to check (linearSearch returned {-1,-1})
        //     break;
        if(recordId.block == -1 && recordId.slot == -1)
          break;
        /* Get the record from the attribute catalog using RecBuffer.getRecord
          into attrCatEntryRecord */
        RecBuffer attrcat(recordId.block);
        attrcat.getRecord(rec,recordId.slot);
        // if attrCatEntryRecord.attrName = oldName
        //     attrToRenameRecId = block and slot of this record
        if(!strcmp(rec[1].sVal,oldName))
        {
            attrToRenameRecId.block = recordId.block;
            attrToRenameRecId.slot = recordId.slot;
            break;
        }
        // if attrCatEntryRecord.attrName = newName
        //     return E_ATTREXIST;
        if(!strcmp(rec[1].sVal,newName))
            return E_ATTREXIST;
    }

    // if attrToRenameRecId == {-1, -1}
    //     return E_ATTRNOTEXIST;
    if(attrToRenameRecId.block == -1 && attrToRenameRecId.slot == -1)
        return E_ATTRNOTEXIST;

    // Update the entry corresponding to the attribute in the Attribute Catalog Relation.
    /*   declare a RecBuffer for attrToRenameRecId.block and get the record at
         attrToRenameRecId.slot */
    //   update the AttrName of the record with newName
    //   set back the record with RecBuffer.setRecord
    RecBuffer recbuff(attrToRenameRecId.block);
    recbuff.getRecord(rec,attrToRenameRecId.slot);
    strcpy(rec[1].sVal,newName);
    recbuff.setRecord(rec,attrToRenameRecId.slot);
    return SUCCESS;
}

int BlockAccess:: insert(int relId, Attribute *record) {
    RelCatEntry relCatEntry;
    RelCacheTable::getRelCatEntry(relId,&relCatEntry);
    int blockNum =(int)relCatEntry.firstBlk;
    RecId recId;
    recId.block=-1;
    recId.slot=-1;
    
    int numOfSlots =(int)relCatEntry.numSlotsPerBlk; /* number of slots per record block */;
    int numOfAttributes =(int) relCatEntry.numAttrs;/* number of attributes of the relation */;
    int prevBlockNum = -1;//relCatEntry.lastBlk; /* block number of the last element in the linked list = -1 */;

    /*
        Traversing the linked list of existing record blocks of the relation
        until a free slot is found OR
        until the end of the list is reached
    */
    struct HeadInfo head;
    unsigned char slotMap[numOfSlots];
    while (blockNum != -1) {
   // printf("%d is blocknum in ba insert\n",blockNum);
        // create a RecBuffer object for blockNum (using appropriate constructor!)
	 RecBuffer recBuffer(blockNum);
 	 recBuffer.RecBuffer::getSlotMap(slotMap);
	 for(int i=0;i<numOfSlots;i++){
        if(slotMap[i]==SLOT_UNOCCUPIED){
        
            recId.block=blockNum;
            recId.slot=i;
             //printf("%d is blocknum grfyk\n",recId.block);
            break;
        }}
	if(recId.block!=-1)
	{
	 break;
	 }
	 recBuffer.getHeader(&head);
	 prevBlockNum = blockNum;
	 blockNum=head.rblock;
	// printf("        %d  ",head.rblock);
        // get header of block(blockNum) using RecBuffer::getHeader() function

        // get slot map of block(blockNum) using RecBuffer::getSlotMap() function

        // search for free slot in the block 'blockNum' and store it's rec-id in rec_id
        // (Free slot can be found by iterating over the slot map of the block)
        /* slot map stores SLOT_UNOCCUPIED if slot is free and
           SLOT_OCCUPIED if slot is occupied) */

        /* if a free slot is found, set rec_id and discontinue the traversal
           of the linked list of record blocks (break from the loop) */

        /* otherwise, continue to check the next block by updating the
           block numbers as follows:
              update prevBlockNum = blockNum
              update blockNum = header.rblock (next element in the linked
                                               list of record blocks)
        */
    }

    //  if no free slot is found in existing record blocks (rec_id = {-1, -1})
    if(recId.block==-1&&recId.slot==-1)
    {
   
        // if relation is RELCAT, do not allocate any more blocks
        if(relId==0)
          return E_MAXRELATIONS;

        // Otherwise,
        // get a new record block (using the appropriate RecBuffer constructor!)
        RecBuffer recBuffer;
        //blockNum=this.blockNum;///POSSIBLE ERROR
        // get the block number of the newly allocated block
        // (use 
       // int x=getFreeBlock(REC);
        int ret=recBuffer.BlockBuffer::getBlockNum();
       
        // let ret be the return value of getBlockNum() function call
        if (ret == E_DISKFULL) {
            return E_DISKFULL;
        }
         //printf("\n%d is blocknum in insert ba hell\n",ret);
	recId.block=ret;
	recId.slot=0;
	head.pblock=REC;
	head.pblock=-1;
	head.lblock=prevBlockNum ;
	head.rblock=-1;
	head.numEntries=0;
	head.numSlots=numOfSlots;
	head.numAttrs=numOfAttributes;
	recBuffer.BlockBuffer::setHeader(&head);
        // Assign rec_id.block = new block number(i.e. ret) and rec_id.slot = 0

        /*
            set the header of the new record block such that it links with
            existing record blocks of the relation
            set the block's header as follows:
            blockType: REC, pblock: -1
            lblock
                  = -1 (if linked list of existing record blocks was empty
                         i.e this is the first insertion into the relation)
                  = prevBlockNum (otherwise),
            rblock: -1, numEntries: 0,
            numSlots: numOfSlots, numAttrs: numOfAttributes
            (use BlockBuffer::setHeader() function)
        */



        for(int i=0;i<numOfSlots;i++){
        slotMap[i]=SLOT_UNOCCUPIED;
        }
        recBuffer.RecBuffer::setSlotMap(slotMap); 
       //  printf("%d is recbuffer blockNUM1",recBuffer.getBlockNum());
        /*
            set block's slot map with all slots marked as free
            (i.e. store SLOT_UNOCCUPIED for all the entries)
            (use RecBuffer::setSlotMap() function)
        */

        // if prevBlockNum != -1
        RelCatEntry relCatEntry;
        RelCacheTable::getRelCatEntry(relId,&relCatEntry);
       if(prevBlockNum!=-1)
        {
       // printf("%d preblock",prevBlockNum);
        RecBuffer recBuffer(prevBlockNum);
        recBuffer.BlockBuffer::getHeader(&head);
        head.rblock=recId.block;
      //  printf("%d is recbuffer blockNUM",recBuffer.getBlockNum());
        recBuffer.BlockBuffer::setHeader(&head);
        recBuffer.getHeader(&head);
    //    printf("%d is headrblock ",head.rblock);
            // create a RecBuffer object for prevBlockNum
            // get the header of the block prevBlockNum and
            // update the rblock field of the header to the new block
            // number i.e. rec_id.block
            // (use BlockBuffer::setHeader() function)
        }
         else
        {
            
           
            relCatEntry.firstBlk=recId.block;
             RelCacheTable::setRelCatEntry(relId,&relCatEntry);
            
            // update first block field in the relation catalog entry to the
            // new block (using RelCacheTable::setRelCatEntry() function)
        }
        relCatEntry.lastBlk=recId.block;
        //relCatEntry.numRecs++;
  
        //  RelCacheTable::setRelCatEntry(relId,&relCatEntry);
          RelCacheTable::setRelCatEntry(relId,&relCatEntry);
         

        // update last block field in the relation catalog entry to the
        // new block (using RelCacheTable::setRelCatEntry() function)
    }
  //  printf("%d is recidblock",recId.block);
    RecBuffer recBuffer(recId.block);
    recBuffer.setRecord(record,recId.slot);
    recBuffer.getHeader(&head);
    // printf("%d is headlblock ",head.lblock);
    head.numEntries=head.numEntries+1;
    recBuffer.setHeader(&head);
    // create a RecBuffer object for rec_id.block
    // insert the record into rec_id'th slot using RecBuffer.setRecord())
    recBuffer.RecBuffer::getSlotMap(slotMap);  
    
    slotMap[recId.slot]=SLOT_OCCUPIED;
    recBuffer.RecBuffer::setSlotMap(slotMap); 
    
   RelCacheTable::getRelCatEntry(relId,&relCatEntry);
   relCatEntry.numRecs++;
   RelCacheTable::setRelCatEntry(relId,&relCatEntry);

   // RecBuffer recBuffer(recId.block);
  
    /* update the slot map of the block by marking entry of the slot to
       which record was inserted as occupied) */
    // (ie store SLOT_OCCUPIED in free_slot'th entry of slot map)
    // (use RecBuffer::getSlotMap() and RecBuffer::setSlotMap() functions)

    // increment the numEntries field in the header of the block to
    // which record was inserted
    // (use BlockBuffer::getHeader() and BlockBuffer::setHeader() functions)

    // Increment the number of records field in the relation cache entry for
    // the relation. (use RelCacheTable::setRelCatEntry function)
    int flag = SUCCESS;
    // Iterate over all the attributes of the relation
    // (let attrOffset be iterator ranging from 0 to numOfAttributes-1)
    AttrCatEntry attrCatEntry;
    for(int i=0;i<relCatEntry.numAttrs;i++)
    {
        // get the attribute catalog entry for the attribute from the attribute cache
        // (use 
        AttrCacheTable::getAttrCatEntry(relId,i,&attrCatEntry); //with args relId and attrOffset)

        // get the root block field from the attribute catalog entry
        int rootBlock=attrCatEntry.rootBlock;
        // if index exists for the attribute
        if( rootBlock != -1)
        {
            /* insert the new record into the attribute's bplus tree using
             BPlusTree::bPlusInsert()*/
            int retVal = BPlusTree::bPlusInsert(relId,attrCatEntry.attrName,record[i],recId);
            if (retVal == E_DISKFULL) {
                //(index for this attribute has been destroyed)
                flag = E_INDEX_BLOCKS_RELEASED;
            }
        }
    }

    return flag;
}

/*
NOTE: This function will copy the result of the search to the `record` argument.
      The caller should ensure that space is allocated for `record` array
      based on the number of attributes in the relation.
*/
/*int BlockAccess::search(int relId, Attribute *record, char attrName[ATTR_SIZE], Attribute attrVal, int op) {
    // Declare a variable called recid to store the searched record
    RecId recId;

    /* search for the record id (recid) corresponding to the attribute with
    attribute name attrName, with value attrval and satisfying the condition op
    using linearSearch() */
/*    recId = linearSearch(relId,attrName,attrVal,op);
    // if there's no record satisfying the given condition (recId = {-1, -1})
    //    return E_NOTFOUND;
    if(recId.block == -1 && recId.slot == -1)
      return E_NOTFOUND;
    /* Copy the record with record id (recId) to the record buffer (record)
       For this Instantiate a RecBuffer class object using recId and
       call the appropriate method to fetch the record
    */
/*    RecBuffer recbuffer(recId.block);
    recbuffer.getRecord(record,recId.slot);
    return SUCCESS;
}*/

int BlockAccess::search(int relId, Attribute *record, char attrName[ATTR_SIZE], Attribute attrVal, int op) {
    // Declare a variable called recid to store the searched record
    RecId recId;

    /* get the attribute catalog entry from the attribute cache corresponding
    to the relation with Id=relid and with attribute_name=attrName  */
    AttrCatEntry attrCatEntry;
    int res = AttrCacheTable::getAttrCatEntry(relId,attrName,&attrCatEntry);
    // if this call returns an error, return the appropriate error code
    if(res != SUCCESS)
      return res;
    // get rootBlock from the attribute catalog entry
    int rootBlock = attrCatEntry.rootBlock;
    if(rootBlock == -1)
    {
        recId = BlockAccess::linearSearch(relId,attrName,attrVal,op);
    }
    
    else{
        // (index exists for the attribute)
        recId = BPlusTree::bPlusSearch(relId,attrName,attrVal,op);
    }


    // if there's no record satisfying the given condition (recId = {-1, -1})
    //     return E_NOTFOUND;
    if(recId.block == -1 && recId.slot == -1){
        return E_NOTFOUND;
    }

    /* Copy the record with record id (recId) to the record buffer (record).
       For this, instantiate a RecBuffer class object by passing the recId and
       call the appropriate method to fetch the record
    */
    RecBuffer recordbuff(recId.block);
    recordbuff.getRecord(record,recId.slot);
    return SUCCESS;
}

int BlockAccess::deleteRelation(char relName[ATTR_SIZE]) {
    // if the relation to delete is either Relation Catalog or Attribute Catalog,
    //     return E_NOTPERMITTED
    if(!strcmp(relName,"RELATIONCAT") || !strcmp(relName,"ATTRIBUTECAT"))
       return E_NOTPERMITTED;
        // (check if the relation names are either "RELATIONCAT" and "ATTRIBUTECAT".
        // you may use the following constants: RELCAT_NAME and ATTRCAT_NAME)

    /* reset the searchIndex of the relation catalog using
       RelCacheTable::resetSearchIndex() */
    RelCacheTable::resetSearchIndex(0);
    Attribute relNameAttr; // (stores relName as type union Attribute)
    // assign relNameAttr.sVal = relName
    strcpy(relNameAttr.sVal,relName);
    char attrName[ATTR_SIZE];
    strcpy(attrName,"RelName");
    //  linearSearch on the relation catalog for RelName = relNameAttr
    RecId recId = linearSearch(0,attrName,relNameAttr,EQ);
    // if the relation does not exist (linearSearch returned {-1, -1})
    //     return E_RELNOTEXIST
    if(recId.block == -1 && recId.slot == -1)
       return E_RELNOTEXIST;
    Attribute relCatEntryRecord[RELCAT_NO_ATTRS];
    /* store the relation catalog record corresponding to the relation in
       relCatEntryRecord using RecBuffer.getRecord */
    RecBuffer recbuffer(recId.block);
    recbuffer.getRecord(relCatEntryRecord,recId.slot);
    /* get the first record block of the relation (firstBlock) using the
       relation catalog entry record */
    int blockNum = relCatEntryRecord[3].nVal;
    /* get the number of attributes corresponding to the relation (numAttrs)
       using the relation catalog entry record */
    int numAttrs =relCatEntryRecord[1].nVal;
    /*
     Delete all the record blocks of the relation
    */
    // for each record block of the relation:
    //     get block header using BlockBuffer.getHeader
    //     get the next block from the header (rblock)
    //     release the block using BlockBuffer.releaseBlock
    //
    //     Hint: to know if we reached the end, check if nextBlock = -1
    HeadInfo head;
    while(blockNum != -1)
    {
       RecBuffer recbuff(blockNum);
       recbuff.getHeader(&head);
       recbuff.releaseBlock();
       blockNum = head.rblock;
    }

    /***
        Deleting attribute catalog entries corresponding the relation and index
        blocks corresponding to the relation with relName on its attributes
    ***/

    // reset the searchIndex of the attribute catalog
    RelCacheTable::resetSearchIndex(1);

    int numberOfAttributesDeleted = 0;

    while(true) {
        RecId attrCatRecId;
        // attrCatRecId = linearSearch on attribute catalog for RelName = relNameAttr
        attrCatRecId = linearSearch(1,attrName,relNameAttr,EQ);
        // if no more attributes to iterate over (attrCatRecId == {-1, -1})
        //     break;
        if(attrCatRecId.block == -1 && attrCatRecId.slot == -1)
          break;

        numberOfAttributesDeleted++;
        Attribute attrRecord[6];
        // create a RecBuffer for attrCatRecId.block
        RecBuffer recbuff(attrCatRecId.block);
        // get the header of the block
        recbuff.getHeader(&head);
        // get the record corresponding to attrCatRecId.slot
        recbuff.getRecord(attrRecord,attrCatRecId.slot);
        // declare variable rootBlock which will be used to store the root
        // block field from the attribute catalog record.
        int rootBlock = attrRecord[4].nVal;
        // (This will be used later to delete any indexes if it exists)

        // Update the Slotmap for the block by setting the slot as SLOT_UNOCCUPIED
        // Hint: use RecBuffer.getSlotMap and RecBuffer.setSlotMap
        unsigned char slotMap[head.numSlots];
        recbuff.getSlotMap(slotMap);
        slotMap[attrCatRecId.slot] = SLOT_UNOCCUPIED;
        recbuff.setSlotMap(slotMap);
        /* Decrement the numEntries in the header of the block corresponding to
           the attribute catalog entry and then set back the header
           using RecBuffer.setHeader */
        head.numEntries = head.numEntries - 1;
        recbuff.setHeader(&head);
        /* If number of entries become 0, releaseBlock is called after fixing
           the linked list.
        */
        if (head.numEntries == 0) {
            /* Standard Linked List Delete for a Block
               Get the header of the left block and set it's rblock to this
               block's rblock
            */
            HeadInfo head1;
   
            // create a RecBuffer for lblock and call appropriate methods
            RecBuffer recbuffer(head.lblock);
            recbuffer.getHeader(&head1);
            head1.rblock = head.rblock;
            recbuffer.setHeader(&head1);

            if (head.rblock != -1) {
                /* Get the header of the right block and set it's lblock to
                   this block's lblock */
                 RecBuffer recBuffer(head.rblock);
                 recBuffer.getHeader(&head1);
                 head1.lblock = head.lblock;
                 recBuffer.setHeader(&head1);
                // create a RecBuffer for rblock and call appropriate methods

            } else {
                // (the block being released is the "Last Block" of the relation.)
                /* update the Relation Catalog entry's LastBlock field for this
                   relation with the block number of the previous block. */
                RelCatEntry relcatentry;
                RelCacheTable::getRelCatEntry(1,&relcatentry);
                relcatentry.lastBlk = head.lblock;
                RelCacheTable::setRelCatEntry(1,&relcatentry);
            }

            // (Since the attribute catalog will never be empty(why?), we do not
            //  need to handle the case of the linked list becoming empty - i.e
            //  every block of the attribute catalog gets released.)
            
            recbuff.releaseBlock();
            // call releaseBlock()
        }

        // (the following part is only relevant once indexing has been implemented)
        // if index exists for the attribute (rootBlock != -1), call bplus destroy
        if (rootBlock != -1) {
            // delete the bplus tree rooted at rootBlock using BPlusTree::bPlusDestroy()
            BPlusTree::bPlusDestroy(rootBlock);
        }
    }

    /*** Delete the entry corresponding to the relation from relation catalog ***/
    // Fetch the header of Relcat block
    RecBuffer recBuffer(RELCAT_BLOCK);
    recBuffer.getHeader(&head);
    head.numEntries = head.numEntries - 1;
    recBuffer.setHeader(&head);
    /* Decrement the numEntries in the header of the block corresponding to the
       relation catalog entry and set it back */
    /* Get the slotmap in relation catalog, update it by marking the slot as
       free(SLOT_UNOCCUPIED) and set it back. */
    unsigned char slotMap[head.numSlots];
    recBuffer.getSlotMap(slotMap);
    slotMap[recId.slot] = SLOT_UNOCCUPIED;
    recBuffer.setSlotMap(slotMap);
    /*** Updating the Relation Cache Table ***/
    /** Update relation catalog record entry (number of records in relation
        catalog is decreased by 1) **/
    RelCatEntry relcatentry;
    RelCacheTable::getRelCatEntry(0,&relcatentry);
    relcatentry.numRecs = relcatentry.numRecs - 1;
    RelCacheTable::setRelCatEntry(0,&relcatentry);
    // Get the entry corresponding to relation catalog from the relation
    // cache and update the number of records and set it back
    // (using RelCacheTable::setRelCatEntry() function)

    /** Update attribute catalog entry (number of records in attribute catalog
        is decreased by numberOfAttributesDeleted) **/
    // i.e., #Records = #Records - numberOfAttributesDeleted
    RelCacheTable::getRelCatEntry(1,&relcatentry);
    relcatentry.numRecs = relcatentry.numRecs - numberOfAttributesDeleted;
    RelCacheTable::setRelCatEntry(1,&relcatentry);
    // Get the entry corresponding to attribute catalog from the relation
    // cache and update the number of records and set it back
    // (using RelCacheTable::setRelCatEntry() function)

    return SUCCESS;
}

/*
NOTE: the caller is expected to allocate space for the argument `record` based
      on the size of the relation. This function will only copy the result of
      the projection onto the array pointed to by the argument.
*/
int BlockAccess::project(int relId, Attribute *record) {
    // get the previous search index of the relation relId from the relation
    // cache (use RelCacheTable::getSearchIndex() function)
    RecId prevRecId;
    RelCacheTable::getSearchIndex(relId,&prevRecId);
    // declare block and slot which will be used to store the record id of the
    // slot we need to check.
    int block, slot;
   
    /* if the current search index record is invalid(i.e. = {-1, -1})
       (this only happens when the caller reset the search index)
    */
    if (prevRecId.block == -1 && prevRecId.slot == -1)
    {
        // (new project operation. start from beginning)

        // get the first record block of the relation from the relation cache
        // (use RelCacheTable::getRelCatEntry() function of Cache Layer)
        RelCatEntry relcatentry;
        RelCacheTable::getRelCatEntry(relId,&relcatentry);
        // block = first record block of the relation
        // slot = 0
        block = relcatentry.firstBlk;
        slot = 0;
    }
    else
    {
        // (a project/search operation is already in progress)

        // block = previous search index's block
        // slot = previous search index's slot + 1
        block = prevRecId.block;
        slot = prevRecId.slot + 1;
    }

    // The following code finds the next record of the relation
    /* Start from the record id (block, slot) and iterate over the remaining
       records of the relation */
    while (block != -1)
    {
        // create a RecBuffer object for block (using appropriate constructor!)
        RecBuffer recordbuf(block);
        // get header of the block using RecBuffer::getHeader() function
        HeadInfo head;
        recordbuf.getHeader(&head);
        // get slot map of the block using RecBuffer::getSlotMap() function
        unsigned char slotMap[head.numSlots];
        recordbuf.getSlotMap(slotMap);
        if(slot >= head.numSlots)
        {
            // (no more slots in this block)
            // update block = right block of block
            block = head.rblock;
            slot = 0;
            continue;
            // update slot = 0
            // (NOTE: if this is the last block, rblock would be -1. this would
            //        set block = -1 and fail the loop condition )
        }
        else if (slotMap[slot] == SLOT_UNOCCUPIED)
        { // (i.e slot-th entry in slotMap contains SLOT_UNOCCUPIED)

            // increment slot
            slot++;
            
        }
        else {
            // (the next occupied slot / record has been found)
            break;
        }
    }
   
    if (block == -1){
        // (a record was not found. all records exhausted)
        return E_NOTFOUND;
    }

    // declare nextRecId to store the RecId of the record found
    RecId nextRecId{block, slot};

    // set the search index to nextRecId using RelCacheTable::setSearchIndex
    RelCacheTable::setSearchIndex(relId,&nextRecId);

    /* Copy the record with record id (nextRecId) to the record buffer (record)
       For this Instantiate a RecBuffer class object by passing the recId and
       call the appropriate method to fetch the record
    */
    RecBuffer recbuffer(nextRecId.block);
    recbuffer.getRecord(record,nextRecId.slot);
   
    return SUCCESS;
}
        

