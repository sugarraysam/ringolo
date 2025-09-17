#include <liburing.h>

// As a library do not expose GFLAG
DECLARE_uint64(io_uring_sqe_ring_size);

// Here debate between implementing this OR using a oneshot channel.
// This is the object returned to client to await response (i.e.: IO completion).
class IoUringAwaitable {
    public:
        explicit IoUringAwaitable(IoBase* ioBase, const std::shared_ptr<IoUring>& ioUring) noexcept : ioBase_(ioBase), ioUring_(ioUring) {}

        bool await_ready() const noexcept {
            return ioBase_->isReady();
        }

        // true  => suspend coroutine
        // false => resume coroutine
        bool await_suspend(std::coroutine_handle<> coroutine) noexcept {
            // Try to mark suspended, if fail then IO must have completed meanwhile
            if (ioBase_->tryMarkSuspended(coroutine)) {
                // Successfully marked as suspended, will be resumed later
                return true;
            }
            // IO already completed, do not suspend
            return false;
        }

        int await_resume() noexcept {
            auto res = ioBase_->getResult();
            ioBase_->markAvailable();
            return res;
        }

    private:
        IoBase* ioBase_;
        std::shared_ptr<IoUring> ioUring_;
};

class IoUring : public std::enable_shared_from_this<IoUring> {
    public:
        explicit IoUring(size_t sqeRingSize = FLAGS_io_uring_sqe_ring_size,
                folly::Executor::KeepAlive<> completionExecutor = folly::getGlobalCPUExecutor())
            : sqeRingSize_(sqeRingSize), completionExecutor_(std::move(completionExecutor)) {

            // TODO: need to create one iouring per thread in our IO thread pool
            // SQ ring size has to be a power of two
            if (sqeRingSize_ & (sqeRingSize_ - 1) != 0) {
                auto old = std::exchange(sqeRingSize_, folly::nextPowTwo(sqeRingSize_));
                XLOGF(WARN, "Rounding up sqeRingSize_ to next power of two: {} -> {}",
                        old, sqeRingSize_);
            }

            // TODO: becomes a MPMC queue
            ioBasePool_.reserve(sqeRingSize_);
            for (auto i = 0; i < sqeRingSize_; i++) {
                ioBasePool_.emplace_back(std::make_unique<IoBase>());
            }

            // TODO: this was the previous staging area
            // TODO: each IO thread will have a MPSC queue, leverage `recv_many`
            pendingSubmit_.wlock()->reserve(64);

            ::memset(&params_, 0, sizeof(params_));
            ::memset(&ring_, 0, sizeof(ring_));

            // TODO: set for each IOthread single thread options and find more options
            params_.flags |= IORING_SETUP_SINGLE_ISSUER;
            params_.flags |= IORING_SETUP_DEFER_TASKRUN;

            if (int ret = ::io_uring_queue_init_params(sqeRingSize_, &ring_, &params_); ret != 0) {
                throw std::runtime_error(std::format("io_uring_queue_init_params failed", {}, ret));
            }
        }

        // TODO: implement 1 async tokio interface per op
        folly::coro::Task<int> co_nop(
                int fd,
                void* buf,
                unsigned int nbytes,
                off_t offset,
                uint8_t sqeFlags = 0) {
            co_return co_await co_generic(IoNop(sqeFlags));
        }

        folly::coro::Task<int> co_read(
                int fd,
                void* buf,
                unsigned int nbytes,
                off_t offset,
                uint8_t sqeFlags = 0) {
            co_return co_await co_generic(IoRead(fd, buf, nbytes, offset, sqeFlags));
        }

        folly::coro::Task<int> co_write(
                int fd,
                void* buf,
                unsigned int nbytes,
                off_t offset,
                uint8_t sqeFlags = 0) {
            co_return co_await co_generic(IoWrite(fd, buf, nbytes, offset, sqeFlags));
        }

        // [DEPRECATED] wont complete inline ever
        int tryComplete() {
            trySubmit();

            if (anyInflight() && anyReadyInCqRing()) {
                if (auto leaderGuard = tryBecomeLeader(); leaderGuard.has_value()) {
                    return complete();
                }
            }

            return 0;
        }

        void incrementNumSuspended() noexcept {
            suspended_.fetch_add(1);

            if (shouldSpawnCompletionTask()) {
                spawnCompletionTask();
            }
        }

    private:
        // [DEPRECATED] IO threads will be spun up at beginning (unless autoscaling implemented)
        // but autoscaling would not be triggered inline
        void spawnCompletionTask() {
            bool expected = false;
            if (!completionTaskRunning_.compare_exchange_strong(expected, true)) {
                return;
            }

            backgroundScope_.add(
                    folly::coro::co_invoke([&]() -> folly::coro::Task<void> {
                        SCOPE_EXIT {
                            completionTaskRunning_.store(false);
                        };

                        do {
                            tryComplete();
                        } while (suspended_.load() >= inflight_.load());

                        co_return;

                    }).scheduleOn(completionExecutor_));
        }

        template <typename T>
        folly::coro::Task<int> co_generic(T&& io) {
            // TODO: replace with dequeuing from MPMC
            // ?? what to do if no available IoBase  -> return EBUSY ??
            auto i = nextIoBaseIndex();
            auto* ioBase = ioBasePool_[i].get();

            ioBase->setIoVariant(std::move(io));
            pendingSubmit_.wlock()->emplace_back(i);

            // TODO: replace with IO thread routing/load balancing
            trySubmit();

            // TODO: replace with oneshot channel? or custom tokio awaitable?
            co_return co_await IoUringAwaitable(ioBase, shared_from_this());
        }


        // [DEPRECATED] was fun prog challenge, lets use battle tested MPMC queue impl
        size_t nextIoBaseIndex() {
            while (true) {
                auto i = ioBasePoolIndex_.fetch_add(1, std::memory_order_relaxed) % sqeRingSize_;

                if (ioBasePool_[i]->tryAcquire()) {
                    return i;
                }
            }
        }

        // TODO: needs to be called by the specific IO thread as this is where the SQ ring lives
        // TODO: after recv from channel
        void prepareSqe(IoBase* ioBase) {
            struct io_uring_sqe* sqe = ::io_uring_get_sqe(&ring_);
            io->prepSqe(sqe);
            ::io_uring_sqe_set_data(sqe, io);
        }

        // [DEPRECATED] will never submit inline
        int trySubmit() {
            if (anyPendingSubmit()) {
                if (auto leaderGuard = tryBecomeLeader(); leaderGuard.has_value()) {
                    return submit();
                }
            }
            return 0;
        }

        int submit() {
            std::vector<size_t> next;
            next.reserve(pendingSubmnit_.rlock()->size() * 2);
            auto toSubmit = pendingSubmit_.exchange(std::move(next));

            {
                // [DEPRECATED] will never submit inline so no need for a lock
                // SQ ring is not thread-safe so we needed a lock
                std::lock_guard<std::mutex> g(lock_);
                for (auto i : toSubmit) {
                    prepareSqe(ioBasePool_[i].get());
                }
            }

            while (true) {
                if (auto res = ::io_uring_submit(&ring_); res >= 0) {
                    break;
                } else if (res == -EBUSY) {
                    XLOG(WARN, "::io_uring_submit failed with -EBUSY. Please double the SQ ring size {}.", sqeRingSize_);
                } else {
                    XLOGF(FATAL, "::io_uring_submit failed: {}", -res);
                }
            }

            inflight_.fetch_add(toSubmit.size());
            return toSubmit.size();
        }

        int complete() {
            std::lock_guard<std::mutex> g(lock_);

            struct io_uring_cqe* cqe;
            unsigned int numCompleted = 0;

            unsigned int head;
            unsigned int n = 0;
            io_uring_for_each_cqe(&ring_ head, cqe) {
                // TODO: exit when we reach MAX_COMPLETIONS, similar to IoUringBackend from folly
                n++;
                if (cqe->user_data) {
                    // TODO: use the reinterpret_cast trick will require `unsafe {}` in Rust
                    // TODO (new): need to return ioBase to our MPMC master queue, will need to keep a shared_ptr to it
                    if (auto* ioBase = reinterpret_cast<IoBase*>(cqe->user_data); ioBase != nullptr) {
                        numCompleted++;
                        if (ioBase->tryMarkReadyAndResume(cqe->res, cqe->flags)) {
                            suspended_.fetch_sub(1);
                        }
                    }
                }
            }
            ::io_uring_cq_advance(&ring_, n);

            // TODO: Possible we still have "inflight" but here we check to submit according
            // to our algorithm.

            if (numCompleted > 0) {
                inflight_.fetch_sub(numCompleted);
            }
            return numCompleted;
        }

        // [DEPRECATED] wont complete/submit inline, no leader election
        using LeaderGuard = decltype(folly::makeGuard(std::function<void()>()));

        std::optional<LeaderGuard> tryBecomeLeader() noexcept {
            bool expected = false;
            bool desired = true;

            if (hasLeader_.compare_exchange_strong(expected, desired)) {
                return std::optional<LeaderGuard>(std::in_place, [&]() {
                    hasLeader_.store(false, std::memory_order_relaxed);
                });
            } else {
                return std::nullopt;
            }
        }

        bool anyPendingSubmit() const noexcept {
            return !pendingSubmit_.rlock()->empty();
        }

        bool anyInflight() const noexcept {
            return inflight_.load() > 0;
        }

        bool anyReadyInCqRing() const noexcept {
            return ;:io_uring_cq_ready(&ring_) > 0;
        }

        // [DEPRECATED]
        bool shouldSpawnCompletionTask() const noexcept {
            return suspended_.load() >= inflight_.load() + pendingSubmit_.rlock()->size();
        }

        size_t sqeRingSize_;
        struct io_uring_params params_;
        struct io_uring ring_;

        std::atomic<size_t> ioBasePoolIndex_{0};
        std::vector<std::unique_ptr<IoBase>> ioBasePool_;

        folly::Synchronized<std::vector<size_t>> pendingSubmit_;

        std::atomic<uint32_t> inflight_{0};
        std::atomic<uint32_t> suspended_{0};

        std::mutex lock_;
        std::atomic_bool hasLeader_{false};

        folly::coro::CancellableAsyncScope backgroundScope_;
        folly::Executor::KeepAlive<> completionExecutor_;
        std::atomic_bool completionTaskRunning_{false};
};
