#include "Buffer/StaticBuffer.h"                                 
#include "Cache/OpenRelTable.h"
#include "Disk_Class/Disk.h"
#include "FrontendInterface/FrontendInterface.h"

#include<iostream>
#include<cstring>

/*int main(int argc, char *argv[]) {
  Disk disk_run;
  
  unsigned char buffer[BLOCK_SIZE];
  int8_t val;
  
  Disk::readBlock(buffer, 0);
  for(int i=0;i<10;i++)
  {
     memcpy(&val,buffer+i,1);
     std::cout<<int(val)<<' ';
  }
  return 0;
}*/

/*int main(int argc, char *argv[]) {
  Disk disk_run;
  StaticBuffer buffer;
  OpenRelTable cache;      
  
  // create objects for the relation catalog and attribute catalog
  RecBuffer relCatBuffer(RELCAT_BLOCK);
  

  HeadInfo relCatHeader;
  
  // load the headers of both the blocks into relCatHeader and attrCatHeader.
  
  // (we will implement these functions later)
   relCatBuffer.getHeader(&relCatHeader);
   
  
  int totalRelations = relCatHeader.numEntries;
  
  
    
    for(int i=0;i<totalRelations;i++){
    
    Attribute relCatRecord[RELCAT_NO_ATTRS]; // will store the record from the relation catalog

    relCatBuffer.getRecord(relCatRecord, i);

    printf("Relation: %s\n", relCatRecord[RELCAT_REL_NAME_INDEX].sVal);
    
    printf("%f\n",relCatRecord[5].nVal);
    
    int num = ATTRCAT_BLOCK;
  
  while(num!=-1) {
  
    RecBuffer attrCatBuffer(num);
    
    HeadInfo attrCatHeader;
    
    attrCatBuffer.getHeader(&attrCatHeader);
    
    int totalAttrs = attrCatHeader.numEntries;

    for (int j = 0 ; j<totalAttrs ; j++) {

      // declare attrCatRecord and load the attribute catalog entry into it
      Attribute attrCatRecord[RELCAT_NO_ATTRS]; 
      attrCatBuffer.getRecord(attrCatRecord, j);
       
      if (attrCatRecord[ATTRCAT_REL_NAME_INDEX].nVal == relCatRecord[RELCAT_REL_NAME_INDEX].nVal) {
        const char *attrType = attrCatRecord[ATTRCAT_ATTR_TYPE_INDEX].nVal == NUMBER ? "NUM" : "STR";
        const char *attrName = attrCatRecord[ATTRCAT_ATTR_NAME_INDEX].sVal;
        printf("%s: %s\n", attrName, attrType);
      }
    }
    
    
    num = attrCatHeader.rblock;
        
    }
    printf("\n");
  }

  return 0;
}*/

int main(int argc, char *argv[]) {
    Disk disk_run;
    StaticBuffer buffer;
    OpenRelTable cache;
    /*for(int i=0;i<=2;++i){
       RelCatEntry relCatEntry;
       int result=RelCacheTable::getRelCatEntry(i,&relCatEntry);
       if (result == SUCCESS) {
            printf("Relation: %s\n", relCatEntry.relName);
            for (int j = 0; j < relCatEntry.numAttrs; j++) {
                AttrCatEntry attrCatEntry;
                int getAttrResult = AttrCacheTable::getAttrCatEntry(i, j, &attrCatEntry);
                if (getAttrResult == SUCCESS) {
                    const char *attrType = attrCatEntry.attrType == NUMBER ? "NUM" : "STR";
                    printf("%s: %s\n", attrCatEntry.attrName, attrType);
                }
                }
        } 
        printf("\n");
    }*/
   
    return FrontendInterface::handleFrontend(argc, argv);
    //return 0;
}
