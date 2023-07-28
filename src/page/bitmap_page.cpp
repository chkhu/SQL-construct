#include "page/bitmap_page.h"

#include "glog/logging.h"

/**
 * TODO: Student Implement
 */
template <size_t PageSize>
bool BitmapPage<PageSize>::AllocatePage(uint32_t &page_offset) {
  for (int i = 0; i < MAX_CHARS; i++) {
    unsigned char test = 1;
    for (int j = 0; j < 8; j++) {
      if ((bytes[i] & test) == 0) {
        bytes[i] += test;
        page_offset = i * 8 + j;
        page_allocated_++;
        return true;
      } else
        test *= 2;
    }
  }
  return false;
}

/**
 * TODO: Student Implement
 */
template <size_t PageSize>
bool BitmapPage<PageSize>::DeAllocatePage(uint32_t page_offset) {
  if (page_offset >= MAX_CHARS * 8) return false;
  uint32_t tem1 = page_offset / 8;
  int tem2 = page_offset - 8 * tem1;
  unsigned char bit_cmp = 1;
  for (int i = 0; i < tem2; i++) bit_cmp *= 2;
  if ((bit_cmp & bytes[tem1]) > 0) {
    bytes[tem1] -= bit_cmp;
    page_allocated_--;
    return true;
  } else
    return false;
}

/**
 * TODO: Student Implement
 */
template <size_t PageSize>
bool BitmapPage<PageSize>::IsPageFree(uint32_t page_offset) const {
  int tem1 = page_offset / 8;
  int tem2 = page_offset - 8 * tem1;
  unsigned char bit_cmp = 1;
  for (int i = 0; i < tem2; i++) bit_cmp *= 2;
  if ((bit_cmp & bytes[tem1]) == 0)
    return true;
  else
    return false;
}

template <size_t PageSize>
bool BitmapPage<PageSize>::IsPageFreeLow(uint32_t byte_index, uint8_t bit_index) const {
  return false;
}

template class BitmapPage<64>;

template class BitmapPage<128>;

template class BitmapPage<256>;

template class BitmapPage<512>;

template class BitmapPage<1024>;

template class BitmapPage<2048>;

template class BitmapPage<4096>;