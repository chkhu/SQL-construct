#include "buffer/clock_replacer.h"

/*
 * @brief constructor
 * @param num_pages the maximum number of pages the ClockReplacer will be required to store
 */
ClockReplacer::ClockReplacer(size_t num_pages)
    :second_chance(num_pages,State::EMPTY),
      pointer(0),
      capacity(num_pages) {}

// dtor
ClockReplacer::~ClockReplacer() {
  //do nothing
}

/**
 * @brief find the first victim
 * @param frame_id
 * @return if there is a victim, return true, otherwise false
 */
bool ClockReplacer::Victim(frame_id_t *frame_id) {
  size_t count_nonempty = 0;
  frame_id_t frame_id_victim = 0;

  for (size_t i = 0; i < capacity; i++) { // traverse the whole clock
    auto id_check = (pointer + i) % capacity;
    switch (second_chance[id_check]) {
      case State::EMPTY:
        continue; // if empty, continue to next iteration
      case State::UNUSED:
        count_nonempty++;
        // get the first victim
        frame_id_victim = (frame_id_victim != 0) ? frame_id_victim : id_check;
        break;
      case State::ACCESSED:
        count_nonempty++;
        second_chance[id_check] = State::UNUSED; // if accessed, set to unused
        break;
    }
  }

  // now we have count_nonempty, frame_id_victim ready

  // all empty, return false
  if (count_nonempty == 0) {
    *frame_id = INVALID_FRAME_ID;
    return false;
  }

  // if no victim, get the first victim
  if (frame_id_victim == 0) {
    for (size_t i = 0; i < capacity; i++) {
      auto id_check = (pointer + i) % capacity;
      if (second_chance[id_check] == State::UNUSED) {
        frame_id_victim = id_check;
        break;
      }
    }
  }

  // set the victim to empty
  second_chance[frame_id_victim] = State::EMPTY;
  pointer = frame_id_victim;
  *frame_id = frame_id_victim;

  return true;
}

/*
 * Pin()
 * @brief pin the frame_id
 * @param frame_id
 * @return void
 *  By setting the frame_id to EMPTY, we can make sure that the frame_id will not be chosen as victim
 */
void ClockReplacer::Pin(frame_id_t frame_id) {
  //remove from replacer
  second_chance[frame_id % capacity] = State::EMPTY;
}

/*
 *  By setting the frame_id to ACCESSED, we can make sure that the frame_id can be chosen as victim
 */
void ClockReplacer::Unpin(frame_id_t frame_id) {
  //add into replacer
  second_chance[frame_id % capacity] = State::ACCESSED;
}

/**
 * @breif count those State != EMPTY
 * @return the current size of replacer
 */

size_t ClockReplacer::Size() {
  return count_if(second_chance.begin(), second_chance.end(), IsEmpty);
}

/**
 * @param itr
 * @return if *itr != State::EMPTY, return true, otherwise false
 */

bool ClockReplacer::IsEmpty(ClockReplacer::State& item) {
  return item != State::EMPTY;
}