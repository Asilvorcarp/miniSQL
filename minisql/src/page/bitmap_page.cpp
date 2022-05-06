#include "page/bitmap_page.h"
//0 for allocated,and 1 for free
template<size_t PageSize>
bool BitmapPage<PageSize>::AllocatePage(uint32_t &page_offset) {
  if(page_allocated_ == MAX_CHARS*8) //full
  {
    return false;
  }
  page_offset = next_free_page_;
  page_allocated_ ++;

  uint32_t byte_index = page_offset / 8;
  uint32_t bit_index = page_offset % 8;
  this->bytes[byte_index] |= (1<<bit_index);
  
  for(uint32_t i = 0;i < MAX_CHARS;i++)
  {
    if(this->bytes[i]<0xff)
    {
      for(uint32_t j = 0;j < 8;j++)
      {
        uint32_t bit_judge = bytes[i]>>j & 1;
        if(bit_judge == 0)
        {
          next_free_page_ = i*8+j;
          break;
        }
      }
      break;
    }
  }
  return true;
}

template<size_t PageSize>
bool BitmapPage<PageSize>::DeAllocatePage(uint32_t page_offset) {
  if(IsPageFree(page_offset)==true){
    return false;
  }
  uint32_t byte_index = page_offset / 8;
  uint32_t bit_index = page_offset % 8;
  bytes[byte_index] -= (1<<bit_index);

  page_allocated_ --;
  for(uint32_t i = 0;i < MAX_CHARS;i++)
  {
    if(this->bytes[i]<0xff)
    {
      for(uint32_t j = 0;j < 8;j++)
      {
        uint32_t bit_judge = bytes[i]>>j & 1;
        if(bit_judge == 0)
        {
          next_free_page_ = i*8+j;
          break;
        }
      }
      break;
    }
  }
  return true;
}

template<size_t PageSize>
bool BitmapPage<PageSize>::IsPageFree(uint32_t page_offset) const {
  uint32_t byte_index = page_offset / 8;
  uint32_t bit_index = page_offset % 8;
  return IsPageFreeLow(byte_index,bit_index);
}
/**
   * check a bit(byte_index, bit_index) in bytes is free(value 0).
   *
   * @param byte_index value of page_offset / 8
   * @param bit_index value of page_offset % 8
   * @return true if a bit is 0, false if 1.
   */
template<size_t PageSize>
bool BitmapPage<PageSize>::IsPageFreeLow(uint32_t byte_index, uint8_t bit_index) const {
  uint32_t bit_judge = this->bytes[byte_index]>>bit_index & 1;
  if(bit_judge==0){
    return true;
  }
  else{
    return false;
  }
}

template
class BitmapPage<64>;

template
class BitmapPage<128>;

template
class BitmapPage<256>;

template
class BitmapPage<512>;

template
class BitmapPage<1024>;

template
class BitmapPage<2048>;

template
class BitmapPage<4096>;