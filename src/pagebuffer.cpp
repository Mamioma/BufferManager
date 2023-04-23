/**
 * @author Additional information regarding authorship and licensing are available in Supplementary.txt
**/

#include <iostream>
#include <memory>

#include "pagebuffer.h"
#include "exceptions_header.h"
#include "exceptions/buffer_exceeded_exception.h"
#include "exceptions/page_pinned_exception.h"
#include "exceptions/bad_buffer_exception.h"
#include "exceptions/hash_not_found_exception.h"
#include "exceptions/page_not_pinned_exception.h"

namespace badgerdb { 

PageBufferManager::PageBufferManager(std::uint32_t buffers)
	: numBufs(buffers) {
	bufferStatTable = new BufferStatus[buffers];

  for (FrameId i = 0; i < buffers; i++) 
  {
  	bufferStatTable[i].frameNo = i;
  	bufferStatTable[i].valid = false;
  }

  pageBufferPool = new Page[buffers];

	int htsize = ((((int) (buffers * 1.2))*2)/2)+1;
  hashTable = new BufHashTbl (htsize);  // allocate the buffer hash table

  clockHand = buffers - 1;
}


PageBufferManager::~PageBufferManager() 
{
	//BEGINNING of your solution -- do not remove this comment

	// flush out all the dirty pages
	for (FrameId i = 0; i < PageBufferManager::numBufs; i++) {
		if (PageBufferManager::bufferStatTable[i].dirty) {
			PageBufferManager::flushFile(PageBufferManager::bufferStatTable[i].file);
		}
	}

	// deallocates the buffer pool and the BufferStatus table
	delete [] PageBufferManager::bufferStatTable;
	delete [] PageBufferManager::pageBufferPool;

	//END of your solution -- do not remove this comment
}

void PageBufferManager::readPage(File* file, const PageId pageNumber, Page*& page)
{
	//BEGINNING of your solution -- do not remove this comment

	// First check whether the page is already in the buffer pool by invoking the lookup() method
	FrameId lookupID;
	try
	{
		PageBufferManager::hashTable->lookup(file, pageNumber, lookupID);
	}
	catch(const HashNotFoundException& e)
	{
		// Page is not in the buffer pool
		// Call allocateBuffer() to allocate a buffer frame
		PageBufferManager::allocateBuffer(lookupID);
		// call the method file->readPage() to read the page from disk into the buffer pool frame
		Page currentPage = file->readPage(pageNumber);
		// insert the page into BufferPool frame
		PageBufferManager::hashTable->insert(file, pageNumber, lookupID);
		// invoke Set() on the frame to set it up properly.
		// Set() will leave the pinCnt for the page set to 1
		PageBufferManager::bufferStatTable[lookupID].Set(file, pageNumber);
		// update the pageBufferPool
		PageBufferManager::pageBufferPool[lookupID] = currentPage;
		// Return a pointer to the frame containing the page via the page parameter.
		page = &currentPage;
		return;
	}

	// Page is in the buffer pool.
	// In this case set the appropriate refbit, increment the pinCnt for the page,
	// and then return a pointer to the frame containing the page via the page parameter.
	PageBufferManager::bufferStatTable[lookupID].refbit = true;
	PageBufferManager::bufferStatTable[lookupID].pinCnt++;
	page = &PageBufferManager::pageBufferPool[lookupID];
	return;

	//END of your solution -- do not remove this comment
}

void PageBufferManager::allocatePage(File* file, PageId &pageNumber, Page*& page) 
{
	//BEGINNING of your solution -- do not remove this comment

	// std::cout << "invoke into allocatePage()" << std::endl;
	// create a page
	Page allocatePage = file->allocatePage();
	page = &allocatePage;
	PageId allocatePageId = page->page_number();
	pageNumber = allocatePageId;
	// std::cout << "allocating a page, number is: " << pageNumber << std::endl;

	// find a frame in buffer pool
	// call allocateBuffer to find a frame using clock algorithm
	FrameId pageFrame;
	PageBufferManager::allocateBuffer(pageFrame);
	// std::cout << "call allocateBuffer to find a frame using clock algorithm, frame Id is: " << pageFrame << std:: endl;

	// check if the dirty bit is set
	if (PageBufferManager::bufferStatTable[pageFrame].dirty)
	{
		// if the dirty bit is set, flush the file back to disk
		PageBufferManager::flushFile(file);
	}

	// if the frame allocated is valid, then remove it from hashTable
	if (PageBufferManager::bufferStatTable[pageFrame].valid) 
	{
		PageId deletePageId = PageBufferManager::bufferStatTable[pageFrame].pageNo;
		PageBufferManager::hashTable->remove(PageBufferManager::bufferStatTable[pageFrame].file, deletePageId);
	}

	// call set() to set up the buffer page status
	PageBufferManager::bufferStatTable[pageFrame].Set(file, pageNumber);

	// insert the key:(file, pageNumber) into hashtable
	PageBufferManager::hashTable->insert(file, pageNumber, pageFrame);

	// store the page acoording to the page frame
	PageBufferManager::pageBufferPool[pageFrame] = *page;

	// std::cout << "return from allocatePage()" << std::endl;

	//END of your solution -- do not remove this comment
}

void PageBufferManager::unPinPage(File* file, const PageId pageNumber, const bool dirty) 
{
	//BEGINNING of your solution -- do not remove this comment

	// std::cout << "invoke into unPinPage()" << std::endl;
	// if page is not found in hashtable, do nothing
	FrameId lookupID;
	try
	{
		PageBufferManager::hashTable->lookup(file, pageNumber, lookupID);
	}
	catch (const HashNotFoundException &e)
	{
		return;
	}

	// if page is found and page pinCnt is already zero, throw PAGENOTPINNED error
	if (PageBufferManager::bufferStatTable[lookupID].pinCnt == 0) {
		throw PageNotPinnedException(file->filename(), pageNumber, lookupID);
	}

	// if page is found and pinCnt is greater than zero,
	// Decrements the pinCnt of the frame containing (file, pageNumber)
	PageBufferManager::bufferStatTable[lookupID].pinCnt--;

	// if dirty bit is true, set the dirty bit
	if (dirty) {
		PageBufferManager::bufferStatTable[lookupID].dirty = true;
	}

	//END of your solution -- do not remove this comment
}

void PageBufferManager::disposePage(File* file, const PageId pageNumber)
{
	//BEGINNING of your solution -- do not remove this comment

	// check to see if the page is allocated a frame in buffer pool
	FrameId lookupID;
	try
	{
		PageBufferManager::hashTable->lookup(file, pageNumber, lookupID);
	}
	catch (const HashNotFoundException &e)
	{
		// if not, then just delete the page
		file->deletePage(pageNumber);
		return;
	}

	// if so, free the page frame
	PageBufferManager::bufferStatTable[lookupID].Clear();
	
	// delete from hashtable
	PageBufferManager::hashTable->remove(file, pageNumber);

	// delete the page
	file->deletePage(pageNumber);
	return;

	//END of your solution -- do not remove this comment
}

void PageBufferManager::advanceClock()
{
	//BEGINNING of your solution -- do not remove this comment

	// clockHand is increased using modular arithmetic so that it does not go past numBufs - 1
	PageBufferManager::clockHand = (PageBufferManager::clockHand + 1) % PageBufferManager::numBufs;

	//END of your solution -- do not remove this comment
}

void PageBufferManager::allocateBuffer(FrameId & frame) 
{
	// BEGINNING of your solution -- do not remove this comment

	int count_Pinned_Page = 0;
	FrameId first_traverse_frameID = PageBufferManager::clockHand;

	// advance clockHand
	PageBufferManager::advanceClock();

	// check if the set is valid
	if (!PageBufferManager::bufferStatTable[PageBufferManager::clockHand].valid)
	{
		// if the frame is invalid, then the frame is unused, return
		frame = PageBufferManager::clockHand;
		return;
	}

	// check if the reference bit is set and whether the frame is pinned
	while (PageBufferManager::bufferStatTable[PageBufferManager::clockHand].refbit ||
			PageBufferManager::bufferStatTable[PageBufferManager::clockHand].pinCnt > 0)
	{
		// if the reference bit is set, convert to false
		if (PageBufferManager::bufferStatTable[PageBufferManager::clockHand].refbit)
		{
			PageBufferManager::bufferStatTable[PageBufferManager::clockHand].refbit = false;
		}

		// check if the frame is pinned, if so, increment count_Pinned_Page
		if (PageBufferManager::bufferStatTable[PageBufferManager::clockHand].pinCnt > 0) {
			count_Pinned_Page++;

			// if all the frames are pinned, then throw an error
			if (count_Pinned_Page == PageBufferManager::numBufs)
			{
				throw BufferExceededException();
			}
		}

		// set count_Pinned_Page to 0, avoiding duplicate pages being counted if clockHand loops back
		if (PageBufferManager::clockHand == first_traverse_frameID)
		{
			count_Pinned_Page = 0;
		}

		// advance the clockHand
		PageBufferManager::advanceClock();

		// check if the set is valid
		if (!PageBufferManager::bufferStatTable[PageBufferManager::clockHand].valid)
		{
			// if the frame is invalid, then the frame is unused, return
			frame = PageBufferManager::clockHand;
			return;
		}

		// check whether all the pages are pinned, if so, throw an error

	}

	// break from the while loop means the current frame's reference bit is false and it is unpinned
	frame = PageBufferManager::clockHand;
	return;

	//END of your solution -- do not remove this comment
}

void PageBufferManager::flushFile(const File* file) 
{
	//BEGINNING of your solution -- do not remove this comment

	// iterate the bufferStatusTable
	for (FrameId i = 0; i < PageBufferManager::numBufs; i++)
	{
		// if the page of the file stored is not what we want, continue
		if (PageBufferManager::bufferStatTable[i].file != file)
		{
			continue;
		}

		// throw a PagePinnedException if some page of the file is pinned
		if (PageBufferManager::bufferStatTable[i].pinCnt > 0)
		{
			std::string nameOfFile = PageBufferManager::bufferStatTable[i].file->filename();
			throw PagePinnedException(nameOfFile, PageBufferManager::bufferStatTable[i].pageNo, i);
		}

		// Throws BadBuffer- Exception if an invalid page belonging to the file is encountered
		if (!PageBufferManager::bufferStatTable[i].valid) 
		{
			throw BadBufferException(i, PageBufferManager::bufferStatTable[i].dirty, 
				PageBufferManager::bufferStatTable[i].valid, PageBufferManager::bufferStatTable[i].refbit);
		}

		// from here the page is all related to the target file
		// if the page is dirty, flush back to disk and set the dirty bit back to false
		if (PageBufferManager::bufferStatTable[i].dirty)
		{
			Page flushPageID = PageBufferManager::pageBufferPool[i];
			PageBufferManager::bufferStatTable[i].file->writePage(flushPageID);
			PageBufferManager::bufferStatTable[i].dirty = false;
		}

		// remove the page from hashTable (whether clean or dirty)
		PageBufferManager::hashTable->remove(PageBufferManager::bufferStatTable[i].file, PageBufferManager::bufferStatTable[i].pageNo);

		// invoke the Clear() method of BufferStatus for the page frame
		// if not clear the status, later may call remove again for the same page then throws an error
		PageBufferManager::bufferStatTable[i].Clear();
	}

	//END of your solution -- do not remove this comment
}

void PageBufferManager::printSelf(void) 
{
  BufferStatus* tempPageBuffer;
	int validFrames = 0;
  
  for (std::uint32_t i = 0; i < numBufs; i++)
	{
  	tempPageBuffer = &(bufferStatTable[i]);
		std::cout << "FrameNo:" << i << " ";
		tempPageBuffer->Print();

  	if (tempPageBuffer->valid == true)
    	validFrames++;
  }

	std::cout << "Total Number of Valid Frames:" << validFrames << "\n";
}

}
