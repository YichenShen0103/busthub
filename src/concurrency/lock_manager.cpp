//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lock_manager.cpp
//
// Identification: src/concurrency/lock_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <set>

#include "concurrency/lock_manager.h"

#include "common/config.h"
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager.h"

namespace bustub {

auto DeleteTxnLockSetForRow(Transaction *txn, LockManager::LockMode &lock_mode, const table_oid_t &oid, const RID &rid)
    -> void {
  if (lock_mode == LockManager::LockMode::SHARED) {
    (*txn->GetSharedRowLockSet())[oid].erase(rid);
    if ((*txn->GetSharedRowLockSet())[oid].empty()) {
      (*txn->GetSharedRowLockSet()).erase(oid);
    }
    return;
  }
  if (lock_mode == LockManager::LockMode::EXCLUSIVE) {
    (*txn->GetExclusiveRowLockSet())[oid].erase(rid);
    if ((*txn->GetExclusiveRowLockSet())[oid].empty()) {
      (*txn->GetExclusiveRowLockSet()).erase(oid);
    }
    return;
  }
}

auto Compatible(const std::set<LockManager::LockMode> &granted_set, const LockManager::LockMode &lock_mode) -> bool {
  if (lock_mode == LockManager::LockMode::INTENTION_SHARED) {
    return granted_set.find(LockManager::LockMode::EXCLUSIVE) == granted_set.end();
  }
  if (lock_mode == LockManager::LockMode::INTENTION_EXCLUSIVE) {
    return granted_set.find(LockManager::LockMode::SHARED) == granted_set.end() &&
           granted_set.find(LockManager::LockMode::SHARED_INTENTION_EXCLUSIVE) == granted_set.end() &&
           granted_set.find(LockManager::LockMode::EXCLUSIVE) == granted_set.end();
  }
  if (lock_mode == LockManager::LockMode::SHARED) {
    return granted_set.find(LockManager::LockMode::INTENTION_EXCLUSIVE) == granted_set.end() &&
           granted_set.find(LockManager::LockMode::SHARED_INTENTION_EXCLUSIVE) == granted_set.end() &&
           granted_set.find(LockManager::LockMode::EXCLUSIVE) == granted_set.end();
  }
  if (lock_mode == LockManager::LockMode::SHARED_INTENTION_EXCLUSIVE) {
    return granted_set.find(LockManager::LockMode::INTENTION_EXCLUSIVE) == granted_set.end() &&
           granted_set.find(LockManager::LockMode::SHARED_INTENTION_EXCLUSIVE) == granted_set.end() &&
           granted_set.find(LockManager::LockMode::EXCLUSIVE) == granted_set.end() &&
           granted_set.find(LockManager::LockMode::SHARED) == granted_set.end();
  }
  if (lock_mode == LockManager::LockMode::EXCLUSIVE) {
    return granted_set.empty();
  }
  return false;  // If the lock mode is not recognized, return false
}

auto AddTxnLockSetForTable(Transaction *txn, LockManager::LockMode &lock_mode, const table_oid_t &oid) -> void {
  if (lock_mode == LockManager::LockMode::SHARED) {
    txn->GetSharedTableLockSet()->insert(oid);
    return;
  }
  if (lock_mode == LockManager::LockMode::EXCLUSIVE) {
    txn->GetExclusiveTableLockSet()->insert(oid);
    return;
  }
  if (lock_mode == LockManager::LockMode::INTENTION_SHARED) {
    txn->GetIntentionSharedTableLockSet()->insert(oid);
    return;
  }
  if (lock_mode == LockManager::LockMode::INTENTION_EXCLUSIVE) {
    txn->GetIntentionExclusiveTableLockSet()->insert(oid);
    return;
  }
  if (lock_mode == LockManager::LockMode::SHARED_INTENTION_EXCLUSIVE) {
    txn->GetSharedIntentionExclusiveTableLockSet()->insert(oid);
    return;
  }
}

auto AddTxnLockSetForRow(Transaction *txn, LockManager::LockMode &lock_mode, const table_oid_t &oid, RID &rid) -> void {
  if (lock_mode == LockManager::LockMode::SHARED) {
    (*txn->GetSharedRowLockSet())[oid].insert(rid);
    return;
  }
  if (lock_mode == LockManager::LockMode::EXCLUSIVE) {
    (*txn->GetExclusiveRowLockSet())[oid].insert(rid);
    return;
  }
}

auto DeleteTxnLockSetForTable(Transaction *txn, LockManager::LockMode &lock_mode, const table_oid_t &oid) -> void {
  if (lock_mode == LockManager::LockMode::SHARED) {
    txn->GetSharedTableLockSet()->erase(oid);
    return;
  }
  if (lock_mode == LockManager::LockMode::EXCLUSIVE) {
    txn->GetExclusiveTableLockSet()->erase(oid);
    return;
  }
  if (lock_mode == LockManager::LockMode::INTENTION_SHARED) {
    txn->GetIntentionSharedTableLockSet()->erase(oid);
    return;
  }
  if (lock_mode == LockManager::LockMode::INTENTION_EXCLUSIVE) {
    txn->GetIntentionExclusiveTableLockSet()->erase(oid);
    return;
  }
  if (lock_mode == LockManager::LockMode::SHARED_INTENTION_EXCLUSIVE) {
    txn->GetSharedIntentionExclusiveTableLockSet()->erase(oid);
    return;
  }
}

auto CheckUpgradeCompatibility(LockManager::LockMode old_mode, LockManager::LockMode new_mode) -> bool {
  // Check if the upgrade is compatible
  switch (old_mode) {
    case LockManager::LockMode::INTENTION_SHARED:
      return true;
    case LockManager::LockMode::SHARED:
      return new_mode == LockManager::LockMode::EXCLUSIVE ||
             new_mode == LockManager::LockMode::SHARED_INTENTION_EXCLUSIVE;
    case LockManager::LockMode::INTENTION_EXCLUSIVE:
      return new_mode == LockManager::LockMode::EXCLUSIVE ||
             new_mode == LockManager::LockMode::SHARED_INTENTION_EXCLUSIVE;
    case LockManager::LockMode::SHARED_INTENTION_EXCLUSIVE:
      return new_mode == LockManager::LockMode::EXCLUSIVE;
    default:
      return false;  // No upgrade allowed from EXCLUSIVE or any other mode
  }
}

auto LockManager::LockTable(Transaction *txn, LockMode lock_mode, const table_oid_t &oid) -> bool {
  // 1 Check if the transaction is already commited or aborted
  if (txn->GetState() == TransactionState::COMMITTED || txn->GetState() == TransactionState::ABORTED) {
    return false;  // Cannot lock if transaction is already done
  }

  // 2 Check if the required lock mode is compatible with the transaction's isolation level and state
  // 2.1 READ_UNCOMMITTED
  if (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED) {
    // The lock mode must be EXCLUSIVE or INTENTION_EXCLUSIVE
    if (lock_mode != LockMode::EXCLUSIVE && lock_mode != LockMode::INTENTION_EXCLUSIVE) {
      // If the lock mode is not compatible, abort the transaction
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_SHARED_ON_READ_UNCOMMITTED);
    }
    if (txn->GetState() == TransactionState::SHRINKING) {
      // If the transaction is in SHRINKING state, it cannot take locks
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
    }
  }
  // 2.2 READ_COMMITTED
  else if (txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) {
    if (txn->GetState() == TransactionState::SHRINKING) {
      // If the transaction is in SHRINKING state, it can only take IS or S locks
      if (lock_mode != LockMode::INTENTION_SHARED && lock_mode != LockMode::SHARED) {
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
      }
    }
  }
  // 2.3 REPEATABLE_READ
  else if (txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ) {
    if (txn->GetState() == TransactionState::SHRINKING) {
      // If the transaction is in SHRINKING state, it cannot take any locks
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
    }
  }

  // 3 Check the table lock map if there is an existing lock request queue for the table
  std::shared_ptr<LockRequestQueue> lock_request_queue;
  table_lock_map_latch_.lock();  // Lock the map before accessing it
  auto it = table_lock_map_.find(oid);
  if (it == table_lock_map_.end()) {
    // If no queue exists, create a new one
    lock_request_queue = std::make_shared<LockRequestQueue>();
    table_lock_map_[oid] = lock_request_queue;
  } else {
    lock_request_queue = it->second;
  }
  std::unique_lock<std::mutex> lock(lock_request_queue->latch_);
  table_lock_map_latch_.unlock();  // Unlock the map latch after accessing the queue

  // 4 Add the transaction to the lock request queue
  bool found = false;  // Flag to check if the transaction is already in the queue
  for (auto it = lock_request_queue->request_queue_.begin(); it != lock_request_queue->request_queue_.end(); ++it) {
    auto request = *it;
    if (request->txn_id_ == txn->GetTransactionId()) {
      // The transaction already exists in the queue
      if (request->lock_mode_ == lock_mode) {
        // If the lock mode is the same, just return true
        lock_request_queue->latch_.unlock();
        return true;
      } else {
        // If the lock mode is different, check if it's an upgrade request and if it's compatible
        if (CheckUpgradeCompatibility(request->lock_mode_, lock_mode)) {
          if (lock_request_queue->upgrading_ != INVALID_TXN_ID) {
            // If another transaction is already upgrading, abort this transaction
            txn->SetState(TransactionState::ABORTED);
            lock_request_queue->latch_.unlock();
            throw TransactionAbortException(txn->GetTransactionId(), AbortReason::UPGRADE_CONFLICT);
          }
          // If compatible, update the lock mode
          lock_request_queue->upgrading_ = txn->GetTransactionId();

          // Remove the old request safely using iterator
          it = lock_request_queue->request_queue_.erase(it);

          // Remove the old lock mode from the transaction's lock set
          DeleteTxnLockSetForTable(txn, request->lock_mode_, oid);

          // Safe delete after erasing from list
          delete request;

          found = true;
          break;  // Exit the loop since we found the transaction
        } else {
          // If not compatible, abort the transaction
          txn->SetState(TransactionState::ABORTED);
          lock_request_queue->latch_.unlock();
          throw TransactionAbortException(txn->GetTransactionId(), AbortReason::INCOMPATIBLE_UPGRADE);
        }
      }
    }
  }
  // If the transaction is not found in the queue, create a new lock request
  auto *new_lock_request = new LockRequest(txn->GetTransactionId(), lock_mode, oid);
  if (!found) {
    lock_request_queue->request_queue_.push_back(new_lock_request);
  } else {
    lock_request_queue->request_queue_.push_front(new_lock_request);  // If found, add to the front of the queue
  }

  while (!lock_request_queue->GrantLockForTable(txn, lock_mode)) {
    if (lock_request_queue->upgrading_ == txn->GetTransactionId()) {
      lock_request_queue->upgrading_ = INVALID_TXN_ID;  // Reset the upgrading transaction ID
    }
    lock_request_queue->cv_.wait(lock);
    if (txn->GetState() == TransactionState::ABORTED) {
      lock_request_queue->request_queue_.remove(new_lock_request);
      delete new_lock_request;
      lock_request_queue->cv_.notify_all();
      return false;
    }
  }
  return true;  // Return true since the lock request has been added to the queue
}

auto LockManager::UnlockTable(Transaction *txn, const table_oid_t &oid) -> bool {
  // 1 Check if the transaction holds a lock on any row in the table
  if (txn->GetSharedRowLockSet()->find(oid) != txn->GetSharedRowLockSet()->end() ||
      txn->GetExclusiveRowLockSet()->find(oid) != txn->GetExclusiveRowLockSet()->end()) {
    // If the transaction holds locks on rows, abort it
    txn->SetState(TransactionState::ABORTED);
    table_lock_map_latch_.unlock();
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::TABLE_UNLOCKED_BEFORE_UNLOCKING_ROWS);
  }

  // 2 Find the lock request queue for the table
  std::shared_ptr<LockRequestQueue> lock_request_queue;
  table_lock_map_latch_.lock();  // Lock the map before accessing it
  auto it = table_lock_map_.find(oid);
  if (it == table_lock_map_.end()) {
    // If no queue exists, return false
    txn->SetState(TransactionState::ABORTED);
    table_lock_map_latch_.unlock();
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
  } else {
    lock_request_queue = it->second;
  }
  table_lock_map_latch_.unlock();  // Unlock the map latch after accessing the queue

  // 3 Remove the transaction from the lock request queue
  std::unique_lock<std::mutex> lock(lock_request_queue->latch_);
  for (auto iter = lock_request_queue->request_queue_.begin(); iter != lock_request_queue->request_queue_.end();
       ++iter) {
    if ((*iter)->txn_id_ == txn->GetTransactionId() && (*iter)->granted_) {
      if ((txn->GetState() == TransactionState::GROWING &&
           txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ &&
           ((*iter)->lock_mode_ == LockMode::EXCLUSIVE || (*iter)->lock_mode_ == LockMode::SHARED)) ||
          (txn->GetState() == TransactionState::GROWING && txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED &&
           ((*iter)->lock_mode_ == LockMode::EXCLUSIVE)) ||
          (txn->GetState() == TransactionState::GROWING &&
           txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED &&
           ((*iter)->lock_mode_ == LockMode::EXCLUSIVE))) {
        txn->SetState(TransactionState::SHRINKING);  // Set the transaction state to SHRINKING
      }
      DeleteTxnLockSetForTable(txn, (*iter)->lock_mode_, oid);  // Remove the lock mode from the transaction's lock set
      auto lock_request = *iter;                                // Store the lock request to be deleted later
      lock_request_queue->request_queue_.erase(iter);           // Erase the lock request from the queue
      delete lock_request;                                      // Delete the lock request
      lock_request_queue->cv_.notify_all();                     // Notify all waiting transactions
      return true;                                              // Return true since the lock request has been removed
    }
  }
  txn->SetState(TransactionState::ABORTED);
  throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
}

auto LockManager::LockRow(Transaction *txn, LockMode lock_mode, const table_oid_t &oid, const RID &rid) -> bool {
  // 1 Check if the lock mode is intention lock
  if (lock_mode == LockManager::LockMode::INTENTION_SHARED || lock_mode == LockManager::LockMode::INTENTION_EXCLUSIVE ||
      lock_mode == LockManager::LockMode::SHARED_INTENTION_EXCLUSIVE) {
    // If it is, abort the transaction
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_INTENTION_LOCK_ON_ROW);
  }

  // 2 Check if the transaction is already commited or aborted
  if (txn->GetState() == TransactionState::COMMITTED || txn->GetState() == TransactionState::ABORTED) {
    return false;  // Cannot lock if transaction is already done
  }

  // 3 Check if the required lock mode is compatible with the transaction's isolation level and state
  // 3.1 READ_UNCOMMITTED
  if (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED) {
    // The lock mode must be EXCLUSIVE or INTENTION_EXCLUSIVE
    if (lock_mode != LockMode::EXCLUSIVE) {
      // If the lock mode is not compatible, abort the transaction
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_SHARED_ON_READ_UNCOMMITTED);
    }
    if (txn->GetState() == TransactionState::SHRINKING) {
      // If the transaction is in SHRINKING state, it cannot take locks
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
    }
  }
  // 3.2 READ_COMMITTED
  else if (txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) {
    if (txn->GetState() == TransactionState::SHRINKING) {
      // If the transaction is in SHRINKING state, it can only take IS or S locks
      if (lock_mode != LockMode::SHARED) {
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
      }
    }
  }
  // 3.3 REPEATABLE_READ
  else if (txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ) {
    if (txn->GetState() == TransactionState::SHRINKING) {
      // If the transaction is in SHRINKING state, it cannot take any locks
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
    }
  }

  // 4 Check the row lock map if there is an existing lock request queue for the table
  table_lock_map_latch_.lock();  // Lock the map before accessing it
  auto it = table_lock_map_.find(oid);
  if (it == table_lock_map_.end()) {
    // If no queue exists, create a new one
    auto new_lock_request_queue = std::make_shared<LockRequestQueue>();
    table_lock_map_[oid] = new_lock_request_queue;
  }
  auto lock_request_queue = table_lock_map_[oid];
  table_lock_map_latch_.unlock();  // Unlock the map latch after accessing the queue

  // 5 Check if the transaction already holds a lock on the table
  bool table_present = false;
  if (lock_mode == LockMode::SHARED) {
    lock_request_queue->latch_.lock();
    for (auto &iter : lock_request_queue->request_queue_) {
      if (iter->txn_id_ == txn->GetTransactionId() && iter->granted_) {
        table_present = true;
        break;
      }
    }
    lock_request_queue->latch_.unlock();
  } else {
    lock_request_queue->latch_.lock();
    for (auto &iter : lock_request_queue->request_queue_) {
      if (iter->txn_id_ == txn->GetTransactionId() && iter->granted_ &&
          (iter->lock_mode_ == LockMode::EXCLUSIVE || iter->lock_mode_ == LockMode::INTENTION_EXCLUSIVE ||
           iter->lock_mode_ == LockMode::SHARED_INTENTION_EXCLUSIVE)) {
        table_present = true;
        break;
      }
    }
    lock_request_queue->latch_.unlock();
  }
  if (!table_present) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::TABLE_LOCK_NOT_PRESENT);
  }

  // 6 Get the row lock request queue for the given RID
  row_lock_map_latch_.lock();  // Lock the row lock map before accessing it
  auto row_lock_map_it = row_lock_map_.find(rid);
  if (row_lock_map_it == row_lock_map_.end()) {
    // If no queue exists, create a new one
    auto new_lock_request_queue = std::make_shared<LockRequestQueue>();
    row_lock_map_[rid] = new_lock_request_queue;
  }
  auto row_lock_request_queue = row_lock_map_[rid];
  row_lock_map_latch_.unlock();  // Unlock the row lock map latch after accessing the queue

  // 7 Add the transaction to the lock request queue
  bool found = false;
  std::unique_lock<std::mutex> lock(row_lock_request_queue->latch_);
  for (auto iter = row_lock_request_queue->request_queue_.begin(); iter != row_lock_request_queue->request_queue_.end();
       ++iter) {
    auto request = *iter;
    if (request->txn_id_ == txn->GetTransactionId() && request->granted_) {
      // The transaction already exists in the queue
      if (request->lock_mode_ == lock_mode) {
        return true;  // If the lock mode is the same, just return true
      } else {
        // If the lock mode is different, check if it's a upgrade request and if it's compatible
        if (CheckUpgradeCompatibility(request->lock_mode_, lock_mode)) {
          if (row_lock_request_queue->upgrading_ != INVALID_TXN_ID) {
            // If another transaction is already upgrading, abort this transaction
            txn->SetState(TransactionState::ABORTED);
            row_lock_request_queue->latch_.unlock();
            throw TransactionAbortException(txn->GetTransactionId(), AbortReason::UPGRADE_CONFLICT);
          }
          // If compatible, update the lock mode, first delete the old request, then insert the new request to the queue
          row_lock_request_queue->upgrading_ = txn->GetTransactionId();
          row_lock_request_queue->request_queue_.erase(iter);  // Erase the iterator safely
          DeleteTxnLockSetForRow(txn, request->lock_mode_, oid,
                                 rid);  // Remove the old lock mode from the transaction's lock set
          delete request;               // Delete the old request
          found = true;                 // Mark that we found the transaction
          break;                        // Exit the loop since we found the transaction
        } else {
          // If not compatible, abort the transaction
          txn->SetState(TransactionState::ABORTED);
          row_lock_request_queue->latch_.unlock();
          throw TransactionAbortException(txn->GetTransactionId(), AbortReason::INCOMPATIBLE_UPGRADE);
        }
      }
    }
  }
  // If the transaction is not found in the queue, create a new lock request
  auto *new_lock_request = new LockRequest(txn->GetTransactionId(), lock_mode, oid, rid);
  if (!found) {
    row_lock_request_queue->request_queue_.push_back(new_lock_request);
  } else {
    row_lock_request_queue->request_queue_.push_front(new_lock_request);  // If found, add to the front of the queue
  }

  while (!row_lock_request_queue->GrantLockForRow(txn, lock_mode)) {
    if (lock_request_queue->upgrading_ == txn->GetTransactionId()) {
      lock_request_queue->upgrading_ = INVALID_TXN_ID;
    }
    lock_request_queue->cv_.wait(lock);
    if (txn->GetState() == TransactionState::ABORTED) {
      lock_request_queue->request_queue_.remove(new_lock_request);
      delete new_lock_request;
      lock_request_queue->cv_.notify_all();
      return false;
    }
  }
  return true;
}

auto LockManager::UnlockRow(Transaction *txn, const table_oid_t &oid, const RID &rid) -> bool {
  row_lock_map_latch_.lock();
  if (row_lock_map_.find(rid) == row_lock_map_.end()) {
    row_lock_map_latch_.unlock();
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
  }
  auto lock_request_queue = row_lock_map_[rid];
  row_lock_map_latch_.unlock();
  std::unique_lock<std::mutex> lock(lock_request_queue->latch_);
  bool is_search = false;
  for (auto it = lock_request_queue->request_queue_.begin(); it != lock_request_queue->request_queue_.end(); ++it) {
    auto iter = *it;
    if (txn->GetTransactionId() == iter->txn_id_ && iter->granted_) {
      is_search = true;
      if ((txn->GetState() == TransactionState::GROWING &&
           txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ) ||
          (txn->GetState() == TransactionState::GROWING && txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED &&
           (iter->lock_mode_ == LockMode::EXCLUSIVE)) ||
          (txn->GetState() == TransactionState::GROWING &&
           txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED && (iter->lock_mode_ == LockMode::EXCLUSIVE))) {
        txn->SetState(TransactionState::SHRINKING);
      }
      lock_request_queue->request_queue_.erase(it);  // Erase the iterator safely
      DeleteTxnLockSetForRow(txn, iter->lock_mode_, oid, rid);
      delete iter;
      break;
    }
  }
  lock.unlock();
  lock_request_queue->cv_.notify_all();
  if (!is_search) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
  }
  return true;
}

void LockManager::AddEdge(txn_id_t t1, txn_id_t t2) {}

void LockManager::RemoveEdge(txn_id_t t1, txn_id_t t2) {}

auto LockManager::HasCycle(txn_id_t *txn_id) -> bool { return false; }

auto LockManager::GetEdgeList() -> std::vector<std::pair<txn_id_t, txn_id_t>> {
  std::vector<std::pair<txn_id_t, txn_id_t>> edges(0);
  return edges;
}

void LockManager::RunCycleDetection() {
  while (enable_cycle_detection_) {
    std::this_thread::sleep_for(cycle_detection_interval);
    {  // TODO(students): detect deadlock
    }
  }
}

auto LockManager::LockRequestQueue::GrantLockForTable(Transaction *txn, LockManager::LockMode lock_mode) -> bool {
  std::set<LockMode> granted_set;
  std::set<LockMode> wait_set;
  LockRequest *lock_request = nullptr;
  for (auto &iter : request_queue_) {
    if (iter->granted_) {
      granted_set.insert(iter->lock_mode_);
    }
    if (iter->txn_id_ == txn->GetTransactionId()) {
      lock_request = iter;
    }
  }
  if (Compatible(granted_set, lock_mode)) {
    if (upgrading_ != INVALID_TXN_ID) {
      if (upgrading_ == txn->GetTransactionId()) {
        upgrading_ = INVALID_TXN_ID;
        if (lock_request != nullptr) {
          lock_request->granted_ = true;
          AddTxnLockSetForTable(txn, lock_mode, lock_request->oid_);
          return true;
        }
      }
      return false;
    }

    for (auto &iter : request_queue_) {
      if (iter->txn_id_ != txn->GetTransactionId()) {
        if (!iter->granted_) {
          wait_set.insert(iter->lock_mode_);
        }
      } else {
        break;
      }
    }
    if (Compatible(wait_set, lock_mode)) {
      if (lock_request != nullptr) {
        lock_request->granted_ = true;
        AddTxnLockSetForTable(txn, lock_mode, lock_request->oid_);
        return true;
      }
    }
  }
  return false;
}

auto LockManager::LockRequestQueue::GrantLockForRow(Transaction *txn, LockManager::LockMode lock_mode) -> bool {
  std::set<LockMode> granted_set;
  std::set<LockMode> wait_set;
  LockRequest *lock_request = nullptr;
  for (auto iter : request_queue_) {
    if (iter->granted_) {
      granted_set.insert(iter->lock_mode_);
    }
    if (iter->txn_id_ == txn->GetTransactionId()) {
      lock_request = iter;
    }
  }
  if (Compatible(granted_set, lock_mode)) {
    if (upgrading_ != INVALID_TXN_ID) {
      if (upgrading_ == txn->GetTransactionId()) {
        upgrading_ = INVALID_TXN_ID;
        if (lock_request != nullptr) {
          lock_request->granted_ = true;
          AddTxnLockSetForRow(txn, lock_mode, lock_request->oid_, lock_request->rid_);
          return true;
        }
      }
      return false;
    }

    for (auto &iter : request_queue_) {
      if (iter->txn_id_ != txn->GetTransactionId()) {
        if (!iter->granted_) {
          wait_set.insert(iter->lock_mode_);
        }
      } else {
        break;
      }
    }
    if (Compatible(wait_set, lock_mode)) {
      if (lock_request != nullptr) {
        lock_request->granted_ = true;
        AddTxnLockSetForRow(txn, lock_mode, lock_request->oid_, lock_request->rid_);
        return true;
      }
    }
  }
  return false;
}

}  // namespace bustub
