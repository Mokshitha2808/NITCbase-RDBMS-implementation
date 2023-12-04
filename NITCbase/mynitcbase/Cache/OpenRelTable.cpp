#include "OpenRelTable.h"

#include <cstring>
#include<cstdlib>
#include<cstdio>

OpenRelTableMetaInfo OpenRelTable::tableMetaInfo[MAX_OPEN];

//BEFORE STAGE 5
OpenRelTable::OpenRelTable() {
  
  
  for (int i = 0; i < MAX_OPEN; ++i) {
    RelCacheTable::relCache[i] = nullptr;
    AttrCacheTable::attrCache[i] = nullptr;
    tableMetaInfo[i].free = true;
  }

  RecBuffer relCatBlock(RELCAT_BLOCK);

  Attribute relCatRecord[RELCAT_NO_ATTRS];
  relCatBlock.getRecord(relCatRecord, RELCAT_SLOTNUM_FOR_RELCAT);

  struct RelCacheEntry relCacheEntry;
  RelCacheTable::recordToRelCatEntry(relCatRecord, &relCacheEntry.relCatEntry);
  relCacheEntry.recId.block = RELCAT_BLOCK;
  relCacheEntry.recId.slot = RELCAT_SLOTNUM_FOR_RELCAT;

  // allocate this on the heap because we want it to persist outside this function
  RelCacheTable::relCache[RELCAT_RELID] = (struct RelCacheEntry*)malloc(sizeof(RelCacheEntry));
  *(RelCacheTable::relCache[RELCAT_RELID]) = relCacheEntry;

  //setting up Attribute Catalog relation in the Relation Cache Table

  relCatBlock.getRecord(relCatRecord, RELCAT_SLOTNUM_FOR_ATTRCAT);
  RelCacheTable::recordToRelCatEntry(relCatRecord, &relCacheEntry.relCatEntry);
  relCacheEntry.recId.block = RELCAT_BLOCK;
  relCacheEntry.recId.slot = RELCAT_SLOTNUM_FOR_ATTRCAT;
  
  RelCacheTable::relCache[ATTRCAT_RELID] = (struct RelCacheEntry*)malloc(sizeof(RelCacheEntry));
  *(RelCacheTable::relCache[ATTRCAT_RELID]) = relCacheEntry;
  
  
  /*HeadInfo relCatHeader;
  relCatBlock.getHeader(&relCatHeader);
  for(int i=0;i<relCatHeader.numEntries;i++)
  {
     relCatBlock.getRecord(relCatRecord, i);
     if(!strcmp(relCatRecord[RELCAT_REL_NAME_INDEX].sVal,"Students"))
     {
        RelCacheTable::recordToRelCatEntry(relCatRecord, &relCacheEntry.relCatEntry);
        relCacheEntry.recId.block = RELCAT_BLOCK;
        relCacheEntry.recId.slot = i;
        RelCacheTable::relCache[i] = (struct RelCacheEntry*)malloc(sizeof(RelCacheEntry));
        *(RelCacheTable::relCache[i]) = relCacheEntry;
     }
  }*/
        
  /************ Setting up Attribute cache entries ************/
  // (we need to populate attribute cache with entries for the relation catalog
  //  and attribute catalog.)

  /**** setting up Relation Catalog relation in the Attribute Cache Table ****/

  RecBuffer attrCatBlock(ATTRCAT_BLOCK);
  Attribute attrCatRecord[ATTRCAT_NO_ATTRS];
  AttrCacheEntry* head = nullptr;
  AttrCacheEntry* temp = nullptr;
  for(int i=0;i<=5;i++)
  {
     AttrCacheEntry* attrCacheEntry = (AttrCacheEntry*)malloc(sizeof(AttrCacheEntry));
     attrCacheEntry->recId.block = ATTRCAT_BLOCK;
     attrCacheEntry->recId.slot = i;
     attrCacheEntry->next = nullptr;
     attrCatBlock.getRecord(attrCatRecord, i);
     AttrCacheTable::recordToAttrCatEntry(attrCatRecord, &attrCacheEntry->attrCatEntry);
     if(head == nullptr)
     {
        head = attrCacheEntry;
        temp = head;
     }
     else
     {
        temp->next = attrCacheEntry;
        temp = temp->next;
     }
  }
  AttrCacheTable::attrCache[RELCAT_RELID] = head;

  // setting up Attribute Catalog relation in the Attribute Cache Table 
  head = nullptr;
  temp = nullptr;
  for(int i=6;i<=11;i++)
  {
     AttrCacheEntry* attrCacheEntry = (AttrCacheEntry*)malloc(sizeof(AttrCacheEntry));
     attrCacheEntry->recId.block = ATTRCAT_BLOCK;
     attrCacheEntry->recId.slot = i;
     attrCacheEntry->next = nullptr;
     attrCatBlock.getRecord(attrCatRecord, i);
     AttrCacheTable::recordToAttrCatEntry(attrCatRecord, &attrCacheEntry->attrCatEntry);
     if(head == nullptr)
     {
        head = attrCacheEntry;
        temp = head;
     }
     else
     {
        temp->next = attrCacheEntry;
        temp=temp->next;
     }
  }
  AttrCacheTable::attrCache[ATTRCAT_RELID] = head;
  
  /*HeadInfo attrCatHeader;
  attrCatBlock.getHeader(&attrCatHeader);
  head = nullptr;
  temp = nullptr;
  for(int i=0;i<attrCatHeader.numEntries;i++)
  {
     attrCatBlock.getRecord(attrCatRecord, i);
     if(!strcmp(attrCatRecord[ATTRCAT_REL_NAME_INDEX].sVal,"Students")){
     AttrCacheEntry* attrCacheEntry = (AttrCacheEntry*)malloc(sizeof(AttrCacheEntry));
     attrCacheEntry->recId.block = ATTRCAT_BLOCK;
     attrCacheEntry->recId.slot = i;
     attrCacheEntry->next = nullptr;
     AttrCacheTable::recordToAttrCatEntry(attrCatRecord, &attrCacheEntry->attrCatEntry);
     if(head == nullptr)
     {
        head = attrCacheEntry;
        temp = head;
     }
     else
     {
        temp->next = attrCacheEntry;
        temp=temp->next;
     }
  }
  }
  AttrCacheTable::attrCache[2] = head;*/
 
  tableMetaInfo[RELCAT_RELID].free = false;
  strcpy(tableMetaInfo[RELCAT_RELID].relName,RELCAT_RELNAME);
  tableMetaInfo[ATTRCAT_RELID].free = false;
  strcpy(tableMetaInfo[ATTRCAT_RELID].relName,ATTRCAT_RELNAME);
  /*tableMetaInfo[2].free = false;
  strcpy(tableMetaInfo[2].relName,"Students");*/
}

int OpenRelTable::getRelId(char relName[ATTR_SIZE]) {

  for(int i=0;i<MAX_OPEN;i++)
  {
    if(!tableMetaInfo[i].free)
    {
       if(!strcmp(relName,tableMetaInfo[i].relName))
          return i;
    }
  }
  return E_RELNOTOPEN;
}

int OpenRelTable::getFreeOpenRelTableEntry() {

  for(int i=2;i<MAX_OPEN;i++)
  {
     if(tableMetaInfo[i].free)
       return i;
  }
  return E_CACHEFULL;
}


int OpenRelTable::openRel(char relName[ATTR_SIZE]) {
  
  int res=OpenRelTable::getRelId(relName);
  if(res>=0){
    return res;
  }

  /* find a free slot in the Open Relation Table
     using OpenRelTable::getFreeOpenRelTableEntry(). */
     
  int relId = OpenRelTable::getFreeOpenRelTableEntry();
  if (relId == E_CACHEFULL){
    return E_CACHEFULL;
  }
  
  // let relId be used to store the free slot.
  

  /****** Setting up Relation Cache entry for the relation ******/

  /* search for the entry with relation name, relName, in the Relation Catalog using
      BlockAccess::linearSearch().
 
      Care should be taken to reset the searchIndex of the relation RELCAT_RELID
      before calling linearSearch().*/

  // relcatRecId stores the rec-id of the relation `relName` in the Relation Catalog.
  RelCacheTable::resetSearchIndex(RELCAT_RELID);
  Attribute attrVal;
  strcpy(attrVal.sVal,relName);
  char attrName[ATTR_SIZE];
  strcpy(attrName,"RelName");
  RecId relcatRecId = BlockAccess::linearSearch(RELCAT_RELID,attrName,attrVal,EQ);
  if (relcatRecId.block == -1 && relcatRecId.slot == -1) {
    // (the relation is not found in the Relation Catalog.)
    return E_RELNOTEXIST;
  }

  /* read the record entry corresponding to relcatRecId and create a relCacheEntry
      on it using RecBuffer::getRecord() and RelCacheTable::recordToRelCatEntry().
      update the recId field of this Relation Cache entry to relcatRecId.
      use the Relation Cache entry to set the relId-th entry of the RelCacheTable.
    NOTE: make sure to allocate memory for the RelCacheEntry using malloc()
  */
  RecBuffer relCatBlock(relcatRecId.block);
  Attribute rec[6];
  relCatBlock.getRecord(rec,relcatRecId.slot);
  struct RelCacheEntry relCacheEntry;
  RelCacheTable::recordToRelCatEntry(rec,&relCacheEntry.relCatEntry);
  relCacheEntry.recId = relcatRecId;
  RelCacheTable::relCache[relId] = (struct RelCacheEntry*)malloc(sizeof(RelCacheEntry));
  *(RelCacheTable::relCache[relId]) = relCacheEntry;
  int numAttrs = relCacheEntry.relCatEntry.numAttrs;
  /****** Setting up Attribute Cache entry for the relation ******/

  // let listHead be used to hold the head of the linked list of attrCache entries.
  AttrCacheEntry* listHead = nullptr;
  AttrCacheEntry* temp = nullptr;
  /*iterate over all the entries in the Attribute Catalog corresponding to each
  attribute of the relation relName by multiple calls of BlockAccess::linearSearch()
  care should be taken to reset the searchIndex of the relation, ATTRCAT_RELID,
  corresponding to Attribute Catalog before the first call to linearSearch().*/
  RecBuffer attrCatBlock(ATTRCAT_BLOCK);
  Attribute attrCatRecord[6];
  RelCacheTable::resetSearchIndex(ATTRCAT_RELID);
  for(int i=0;i<numAttrs;i++)
  {
      /* let attrcatRecId store a valid record id an entry of the relation, relName,
      in the Attribute Catalog.*/
    
      RecId attrcatRecId = BlockAccess::linearSearch(ATTRCAT_RELID,attrName,attrVal,EQ);
      AttrCacheEntry* attrCacheEntry = (AttrCacheEntry*)malloc(sizeof(AttrCacheEntry));
      attrCacheEntry->recId.block = attrcatRecId.block;
      attrCacheEntry->recId.slot = attrcatRecId.slot;
      attrCacheEntry->next = nullptr;
      RecBuffer attrCatBlock(attrcatRecId.block);
      attrCatBlock.getRecord(attrCatRecord,attrcatRecId.slot);
      AttrCacheTable::recordToAttrCatEntry(attrCatRecord, &attrCacheEntry->attrCatEntry);
      if(listHead == nullptr)
     {
        listHead = attrCacheEntry;
        temp = listHead;
     }
     else
     {
        temp->next = attrCacheEntry;
        temp=temp->next;
     }
      /* read the record entry corresponding to attrcatRecId and create an
      Attribute Cache entry on it using RecBuffer::getRecord() and
      AttrCacheTable::recordToAttrCatEntry().
      update the recId field of this Attribute Cache entry to attrcatRecId.
      add the Attribute Cache entry to the linked list of listHead .*/
      // NOTE: make sure to allocate memory for the AttrCacheEntry using malloc()
      
  }
  AttrCacheTable::attrCache[relId] = listHead;
  // set the relIdth entry of the AttrCacheTable to listHead.

  /****** Setting up metadata in the Open Relation Table for the relation******/

  // update the relIdth entry of the tableMetaInfo with free as false and
  // relName as the input.
  
  tableMetaInfo[relId].free = false;
  strcpy(tableMetaInfo[relId].relName,relName);
  return relId;
}

/*int OpenRelTable::closeRel(int relId) {
  if (relId == 0 || relId == 1) {
    return E_NOTPERMITTED;
  }

  if (relId<0 || relId>=MAX_OPEN) {
    return E_OUTOFBOUND;
  }

  if (tableMetaInfo[relId].free) {
    return E_RELNOTOPEN;
  }

  // free the memory allocated in the relation and attribute caches which was
  // allocated in the OpenRelTable::openRel() function
  RelCacheTable::relCache[relId] = nullptr;
  AttrCacheTable::attrCache[relId] = nullptr;
  tableMetaInfo[relId].free = true;
  // update `tableMetaInfo` to set `relId` as a free slot
  // update `relCache` and `attrCache` to set the entry at `relId` to nullptr

  return SUCCESS;
}*/

int OpenRelTable::closeRel(int relId) {
  // confirm that rel-id fits the following conditions
  //     2 <=relId < MAX_OPEN
  //     does not correspond to a free slot
  //  (you have done this already)
  if (relId == 0 || relId == 1) {
    return E_NOTPERMITTED;
  }

  if (relId<0 || relId>=MAX_OPEN) {
    return E_OUTOFBOUND;
  }

  if (tableMetaInfo[relId].free) {
    return E_RELNOTOPEN;
  }
  /****** Releasing the Relation Cache entry of the relation ******/

  if (RelCacheTable::relCache[relId]->dirty == true)
  {

    /* Get the Relation Catalog entry from RelCacheTable::relCache
    Then convert it to a record using RelCacheTable::relCatEntryToRecord(). */
    struct RelCacheEntry* relCacheEntry = RelCacheTable::relCache[relId];
    Attribute Record[RELCAT_NO_ATTRS];
    RelCacheTable::relCatEntryToRecord(&relCacheEntry->relCatEntry,Record);
    RecId recId = relCacheEntry->recId;
    //printf("%d",recId.block);
    // declaring an object of RecBuffer class to write back to the buffer
    RecBuffer relCatBlock(recId.block);
    relCatBlock.setRecord(Record,recId.slot);
    // Write back to the buffer using relCatBlock.setRecord() with recId.slot
  }
  RelCacheTable::relCache[relId] = nullptr;

 /* /****** Releasing the Attribute Cache entry of the relation ******/

  // free the memory allocated in the attribute caches which was
  // allocated in the OpenRelTable::openRel() function

  // (because we are not modifying the attribute cache at this stage,
  // write-back is not required. We will do it in subsequent
  // stages when it becomes needed)
  /*AttrCacheTable::attrCache[relId] = nullptr;*/
    
    for(AttrCacheEntry* entry = AttrCacheTable::attrCache[relId]; entry != nullptr; entry = entry->next)
    {
        if(entry->dirty == true)
        {
            /* Get the Attribute Catalog entry from attrCache
             Then convert it to a record using AttrCacheTable::attrCatEntryToRecord().*/
            Attribute record[6];
            AttrCacheTable::attrCatEntryToRecord(&entry->attrCatEntry,record);
             /*Write back that entry by instantiating RecBuffer class. Use recId
             member field and recBuffer.setRecord() */
            RecBuffer recBuffer(entry->recId.block);
            recBuffer.setRecord(record,entry->recId.slot);
        }

        // free the memory dynamically alloted to this entry in Attribute
        // Cache linked list and assign nullptr to that entry 
       /* AttrCacheEntry *en=entry;
        entry=entry->next;
        en=nullptr;*/
    }
    AttrCacheTable::attrCache[relId] == nullptr;
    
  /****** Set the Open Relation Table entry of the relation as free ******/

  // update `metainfo` to set `relId` as a free slot
  tableMetaInfo[relId].free = true;
  return SUCCESS;
}

/*OpenRelTable::~OpenRelTable() {
  for (int i = 2; i < MAX_OPEN; ++i) {
    if (!tableMetaInfo[i].free) {
      OpenRelTable::closeRel(i); // we will implement this function later
    }
  }
  RelCacheTable::relCache[0] = nullptr;
  RelCacheTable::relCache[1] = nullptr;
  AttrCacheTable::attrCache[0] = nullptr;
  AttrCacheTable::attrCache[1] = nullptr;  
  tableMetaInfo[0].free = true;
  tableMetaInfo[1].free = true;
  
  // free the memory allocated for rel-id 0 and 1 in the caches
}*/

OpenRelTable::~OpenRelTable()
{
   for (int i = 2; i < MAX_OPEN; ++i) {
   if (!tableMetaInfo[i].free) {
      OpenRelTable::closeRel(i); // we will implement this function later
   }
   }

    /**** Closing the catalog relations in the relation cache ****/

    //releasing the relation cache entry of the attribute catalog

    if (RelCacheTable::relCache[1]->dirty) {

        /* Get the Relation Catalog entry from RelCacheTable::relCache
        Then convert it to a record using RelCacheTable::relCatEntryToRecord(). */
        struct RelCacheEntry* relCacheEntry = RelCacheTable::relCache[1];
        Attribute Record[RELCAT_NO_ATTRS];
        RelCacheTable::relCatEntryToRecord(&relCacheEntry->relCatEntry,Record);
        RecId recId = relCacheEntry->recId;
        // declaring an object of RecBuffer class to write back to the buffer
        RecBuffer relCatBlock(recId.block);
        relCatBlock.setRecord(Record,recId.slot);
        // Write back to the buffer using relCatBlock.setRecord() with recId.slot
    }
    // free the memory dynamically allocated to this RelCacheEntry
    RelCacheTable::relCache[1] = nullptr;

    //releasing the relation cache entry of the relation catalog

    if(RelCacheTable::relCache[0]->dirty) {

        /* Get the Relation Catalog entry from RelCacheTable::relCache
        Then convert it to a record using RelCacheTable::relCatEntryToRecord(). */
        struct RelCacheEntry* relCacheEntry = RelCacheTable::relCache[0];
        Attribute Record[RELCAT_NO_ATTRS];
        RelCacheTable::relCatEntryToRecord(&relCacheEntry->relCatEntry,Record);
        RecId recId = relCacheEntry->recId;
        // declaring an object of RecBuffer class to write back to the buffer
        RecBuffer relCatBlock(recId.block);
        relCatBlock.setRecord(Record,recId.slot);
        // Write back to the buffer using relCatBlock.setRecord() with recId.slot
    }
    // free the memory dynamically allocated for this RelCacheEntry
    RelCacheTable::relCache[0] = nullptr;
    AttrCacheTable::attrCache[0] = nullptr;
    AttrCacheTable::attrCache[1] = nullptr;  
    tableMetaInfo[0].free = true;
    tableMetaInfo[1].free = true;
    // free the memory allocated for the attribute cache entries of the
    // relation catalog and the attribute catalog
}


