////////////////////////////////////////////////////////////////////////////////
//                   
// Main Class File:  testbuf.c
// File:             buf.c
// Semester:         CS564 Fall 2022
//
// Lecturer's Name:  Paris Koutris
//
////////////////////////////////////////////////////////////////////////////////
//
// Student 1: Zachary Osborn
// Student ID: 9083623414
// 
// Student 2: Nhut Ly
// Student ID: 9080834758
//
// Student 3: Ahnaf Abrar Kabir
// Student ID: 9081504244
// 
//
//////////////////////////// 80 columns wide ///////////////////////////////////

////////////////////////////////////////////////////////////////////////////////
// PURPOSE OF buf.c FILE
// buf.c is a buffer manager for our database system also known as "Minirel". 
// 
// The buffer manager is used to control which pages are memory resident. 
// When the buffer manager receives a request for a data page, the buffer manager 
// will check to see whether the requested page is already in the one of the frames 
// that constitute the buffer pool. 
//      1) If so, the buffer manager simply returns a pointer to the page. 
//      2) If not, the buffer manager frees a frame (possibly by writing to disk 
//        the page it contains if the page is dirty) and then reads in the requested page 
//        from disk into the frame that has been freed.
//  
//
//
////////////////////////////////////////////////////////////////////////////////

#include <memory.h>
#include <unistd.h>
#include <errno.h>
#include <stdlib.h>
#include <fcntl.h>
#include <iostream>
#include <stdio.h>
#include "page.h"
#include "buf.h"

#define ASSERT(c)  { if (!(c)) { \
		       cerr << "At line " << __LINE__ << ":" << endl << "  "; \
                       cerr << "This condition should hold: " #c << endl; \
                       exit(1); \
		     } \
                   }

//----------------------------------------
// Constructor of the class BufMgr
//----------------------------------------

BufMgr::BufMgr(const int bufs)
{
    numBufs = bufs;

    bufTable = new BufDesc[bufs]; // stores the metadata
    memset(bufTable, 0, bufs * sizeof(BufDesc));
    for (int i = 0; i < bufs; i++) 
    {
        bufTable[i].frameNo = i;
        bufTable[i].valid = false;
    }

    bufPool = new Page[bufs]; // the actual hashTable, 
                              // meta-data of this si stored in the 
                              // corresponding index of the `bufTable` 
    memset(bufPool, 0, bufs * sizeof(Page));

    int htsize = ((((int) (bufs * 1.2))*2)/2)+1;
    hashTable = new BufHashTbl (htsize);  // allocate the buffer hash table

    clockHand = bufs - 1;
}

//----------------------------------------
//  Flusher of the buffer:
//	1) Flushes out all dirty pages 
//  2) Deallocates the buffer pool and the 
//	BufDesc table.
//----------------------------------------
BufMgr::~BufMgr() {

    // flush out all unwritten pages
    for (int i = 0; i < numBufs; i++) 
    {
        BufDesc* tmpbuf = &bufTable[i];
        if (tmpbuf->valid == true && tmpbuf->dirty == true) {

        #ifdef DEBUGBUF
            cout << "flushing page " << tmpbuf->pageNo
                 << " from frame " << i << endl;
        #endif

            tmpbuf->file->writePage(tmpbuf->pageNo, &(bufPool[i]));
        }
    }

    delete [] bufTable;
    delete [] bufPool;
}

/*
 *  Used to allocate a free frame using the Clock algorithm.
 *  if the frame is dirty (dirty bit = 1), write it to the disk
 *  @param frame
 *  @return status: 
 *      BUFFEREXCEEDED: if all buffer frames are pinned
 *      UNIXERR: if the call to the I/O layer returned an error when a dirty page was being written to disk
 *      OK: if none of the above applied
 *  
 */
 
const Status BufMgr::allocBuf(int & frame) 
{
    Status status = OK;
    bool foundFrame = false;
    int visitedCount = 0;

    //iterates through every frame, we iteraters through the buffer twice since in the first iteration.
    //if we have not found any available frame, we have set some of the frame's refbit to false.
    //therefore it will make a difference in the second try.
    while(visitedCount < 2*numBufs){
        advanceClock();
        visitedCount++;
        // if a frame is invalid, we use it
        if(bufTable[clockHand].valid == false){
            break;
        }

        // else if the frame is valid, check if the refBit is set
        // if the refbit is not set, that means the frame has NOT been referenced "recently"
        // check if the page is pinned
        if(bufTable[clockHand].refbit == false){
            //check if the page is pinned
            if(bufTable[clockHand].pinCnt == 0){
                //this frame is chosen for replacement since it has not been recently referenced and not pinned
                hashTable->remove(bufTable[clockHand].file, bufTable[clockHand].pageNo);
                foundFrame = true;
                break;
            }
        }
        //else if the refbit is set, we clear the refbit
        else{
            //when the frame has been recently referenced, we clear the reference bit, and advance the clock
            bufTable[clockHand].refbit = false;
            bufStats.accesses++;
        }
        
    }
    
    //after iterating through every frame in the buffer

    //check if we found an available frame and if the buffer is full
    //if true return BUFFEREXCEEDED
    if(foundFrame == false && visitedCount >= 2*numBufs){
        status = BUFFEREXCEEDED;
        return status;
    }

    //check if the page we are going to use is dirty
    //if so, write it to disk
    if(bufTable[clockHand].dirty == true){
        status = bufTable[clockHand].file->writePage(bufTable[clockHand].pageNo,
                                                     &bufPool[clockHand]);
        bufStats.diskwrites++;
        if(status != OK){
            return status;
        }
    }

    frame = clockHand;
    return status;
}

/*
 *  Used to read a page from the buffer pool
 *  If the page is in the buffer pool just return pointer
 *  If the page is not in the buffer pool, add page then return pointer
 * 
 *  @param file - the file address that the page is assosiated with page
 *  @param pageNo - the page number assosiated with the file
 *  @param page - pointer to the page in buffer pool
 *  @return status: 
 *      BUFFEREXCEEDED: if error occured with the buffer allocator
 *      HASHTBLERROR: if error occured with the hash table function
 *      UNIXERR: if the call to the I/O layer returned an error
 *      OK: if none of the above applied
 *  
 */
const Status BufMgr::readPage(File* file, const int pageNo, Page*& page)
{
    bool inPool = false;
    int frameNo;
    Status status = OK; 

    //Use lookup() to see if the page is in the pool 
    //Checks to see if the page is is currently ing the hash table
    //Changes the inPool status to match if the page is in table or not
    status = hashTable->lookup(file, pageNo, frameNo);
    if (status != OK) {
        inPool = false;
    } else {
        inPool = true;
    }

    //case 1, page is not in the buffer pool
    //Covers the first case of the page is not in the buffer pool
    //For this case we need to put the page into the buffer pool and then return that page
    if (!inPool){

        //Call allocBuf()
        //Allocate a frame within the buffer for our page to sit 
        //Return BUFFEREXCEEDED if the buffer pool is already full
        status = allocBuf(frameNo);
        if (status != OK) {
            return status;
        }
        //Update the I/O reading since we are access the disk to move the page to the buffer pool
        bufStats.diskreads++;

        //call file->readPage() 
        // Read the page from disk and move to the buffer pool
        // return UNIXERR if we can not read the page from disk
        status = file->readPage(pageNo, &bufPool[frameNo]);
        if (status != OK) {
            return status;
        }

        //finally invoke Set()
        //Initialize the page we just put in the buffer pool
        //Update the page pointer for return
        bufTable[frameNo].Set(file, pageNo);
        page = &bufPool[frameNo];

        //insert page into hashtable
        //inserts the page into the hash table
        //Return HASHTBLERROR if there is a problem allocating to the hashtable
        status = hashTable->insert(file, pageNo, frameNo);
        if (status != OK){
            return status;
        }

    } 
    else {
    //case 2, page is in the buffer pool
    // Since page is already in the buffer pool 
    // Change the rebit to true
    // increase the pin count of the page

        //udpate refBit 
        //change refBit to true since our request is accessing that page
        bufTable[frameNo].refbit = true;

        //increment the pinCount
        //increase the pin count since we are reading the page
        bufTable[frameNo].pinCnt += 1;

        //Initialize page pointer for return
        page = &bufPool[frameNo];
    }

    //Return OK if the read function worked without errors
    return status;
}

/*
 *  Decrements the pinCnt of the frame containing (file, PageNo) and 
 *  if dirty == true, sets the dirty bit.
 *  @param file - the file address that the page is assosiated with page
 *  @param pageNo - the page number assosiated with the file
 *  @param dirty - if true indicates that the page is dirty (has been updated) and needs to be written to disk
 *  @return status: 
 *      PAGENOTPINNED: if the pin count is already zero
 *      HASHNOTFOUND: if the page is not in the buffer pool hash table
 *      OK: if no errors occured
 *  
 */
const Status BufMgr::unPinPage(File* file, const int PageNo, const bool dirty) 
{
    int frameNo; // gets updated during call to lookup()
    //Set up status 
    Status status; 

    // Use lookup() to see if the page is in the pool 
    // return HASHNOTFOUND lookup fails
    status = hashTable->lookup(file, PageNo, frameNo);
    if (status != OK) {
        status = HASHNOTFOUND;
        return status;
    }

    // check if the page is pinned
    // return PAGENOTPINNED for unpinned pages
    if (bufTable[frameNo].pinCnt < 1){
        status = PAGENOTPINNED;
        return status;
    }

    // Decrement the pinCnt
    else{
        bufTable[frameNo].pinCnt -= 1;
    }
   

    // set the dirty bit
    if (dirty == true){
        bufTable[frameNo].dirty = dirty;
    }
    

    // Return OK if funciton executes without errors
    return OK;
}

/*
 *  Used to allocate a page to frame in the buffer pool.
 *  @param file - the file address that the page is assosiated with page
 *  @param pageNo - the page number assosiated with the file
 *  @param page - the chuck of data being moved to the buffer pool
 *  @return status: 
 *      BUFFEREXCEEDED: if error occured with the buffer allocator
 *      HASHTBLERROR: if error occured with the hash table function
 *      UNIXERR: if the call to the I/O layer returned an error
 *      OK: if none of the above applied
 *  
 */
const Status BufMgr::allocPage(File* file, int& pageNo, Page*& page)
{   
    //Set up status 
    Status status = OK;
     
    //allocate new page with specified file, use  file->allocatePage()
    // Read the page from file and allocate it a page number
    status  = file->allocatePage(pageNo);
    if (status != OK){
       return status;
    }
 
    //call allocBuf to get frame use allocBuf()
    // Find a frame within the buffer pool to add the page, ge
    //Return BUFFEREXCEEDED if all buffer frames are pinned
    int frameNum;
    status = allocBuf(frameNum);
    if (status != OK){
        return status;
    }
    
    //invoke Set()
    // Initialize the page as a new page in the buffer pool 
    bufTable[frameNum].Set(file, pageNo);

    //Initialize page pointer for return
    page = &bufPool[frameNum];
    
    //insert page into hash table
    // Add the page to the hash table for accessing later
    //Return HASHTBLERROR if hash table error occurred 
    status = hashTable->insert(file, pageNo, frameNum);
    if (status != OK){
        return status;
    }
     
    //Return status OK is there was no errors within function
    return status;
    
}

const Status BufMgr::disposePage(File* file, const int pageNo) 
{
    // see if it is in the buffer pool
    Status status = OK;
    int frameNo = 0;
    status = hashTable->lookup(file, pageNo, frameNo);
    if (status == OK)
    {
        // clear the page
        bufTable[frameNo].Clear();
    }
    status = hashTable->remove(file, pageNo);

    // deallocate it in the file
    return file->disposePage(pageNo);
}


/*
 *  Scan bufTable arrays for pages belonging to 
 *  the provided file in the argument. For every page encountered it does the following:
 *  
 *  1) if the page is dirty, call file->writePage() to flush the page to 
 *      disk and then set false the dirty bit of the page.
 *  2) remove the page from the hashtable
 *  3) call the Clear() method on the page frame.
 *  
 *  @param  a file
 *  @returns OK if no errors occurred 
 *           PAGEPINNED if some page is pinned.
 */
const Status BufMgr::flushFile(const File* file) 
{
  Status status;

  for (int i = 0; i < numBufs; i++) {
    BufDesc* tmpbuf = &(bufTable[i]);
    if (tmpbuf->valid == true && tmpbuf->file == file) {

      if (tmpbuf->pinCnt > 0)
	  return PAGEPINNED;

      if (tmpbuf->dirty == true) {
#ifdef DEBUGBUF
	cout << "flushing page " << tmpbuf->pageNo
             << " from frame " << i << endl;
#endif
	if ((status = tmpbuf->file->writePage(tmpbuf->pageNo,
					      &(bufPool[i]))) != OK)
	  return status;

	tmpbuf->dirty = false;
      }

      hashTable->remove(file,tmpbuf->pageNo);

      tmpbuf->file = NULL;
      tmpbuf->pageNo = -1;
      tmpbuf->valid = false;
    }

    else if (tmpbuf->valid == false && tmpbuf->file == file)
      return BADBUFFER;
  }
  
  return OK;
}

/**
 * Prints buffer
 * 
 */
void BufMgr::printSelf(void) 
{
    BufDesc* tmpbuf;
  
    cout << endl << "Print buffer...\n";
    for (int i=0; i<numBufs; i++) {
        tmpbuf = &(bufTable[i]);
        cout << i << "\t" << (char*)(&bufPool[i]) 
             << "\tpinCnt: " << tmpbuf->pinCnt;
    
        if (tmpbuf->valid == true)
            cout << "\tvalid\n";
        cout << endl;
    };
}


