// This file defines an IoWrapper class to wrap all iouring supported operations.
// The strategy was to allocate the IoWrapper once, and create an object pool. It was stored in a variant
// so the variant would allocate enough memory for the largest object.
// Then we reuse the IoWrapper, and ensure we override copy-only cheap fields to avoid memory allocations.

// See all supported iouring operations:
enum class IoType : uint8_t {
    NOP = 0, // Supported default NOP
    READ,
    WRITE,
};

// This is our main interface. It uses CRTP to avoid virtual dispatch.
// We use static assert to enfore `prepSqe` is implemented on all derived classes.
template <typename Derived>
class IoWrapper {
pubic:
    void prepSqe(io_uring_sqe* sqe) {
        static_assert(std::is_invocable_v<decltype(&Derived::prepSqe), Derived, io_uring_sqe*>,
                      "Derived class must implement prepSqe(io_uring_sqe* sqe)");
        static_cast<Derived*>(this)->prepSqe(sqe);
    }
private:
    // Make constructor private to avoid human typo when implementing interface
    friend Derived;
    IoWrapper(IoType type, uint8_t sqeFlags = 0) : type(type), sqeFlags(sqeFlags) {}

    IoType type;
    uint8_t sqeFlags{0};
};

// --- Here we implement all supported flavors of IoWrapper ---
struct IoNop : public IoWrapper<IoNop> {
    IoNop()
        : IoWrapper(IoType::NOP, sqeFlags) {}

    void prepSqe(io_uring_sqe* sqe) {
        ::io_uring_prep_nop(sqe);
    }
};

struct IoRead : public IoWrapper<IoRead> {
    IoRead(int fd, void* buf, size_t nbytes, off_t offset, uint8_t sqeFlags = 0)
        : IoWrapper(IoType::READ, sqeFlags), fd(fd), buf(buf), nbytes(nbytes), offset(offset) {}

    void prepSqe(io_uring_sqe* sqe) {
        ::io_uring_prep_read(sqe, fd, buf, nbytes, offset);
    }

    int fd;
    void* buf;
    size_t nbytes;
    off_t offset;
};


struct IoWrite : public IoWrapper<IoWrite> {
    IoWrite(int fd, void* buf, size_t nbytes, off_t offset, uint8_t sqeFlags = 0)
        : IoWrapper(IoType::WRITE, sqeFlags), fd(fd), buf(buf), nbytes(nbytes), offset(offset) {}

    void prepSqe(io_uring_sqe* sqe) {
        ::io_uring_prep_write(sqe, fd, buf, nbytes, offset);
    }

    int fd;
    void* buf;
    size_t nbytes;
    off_t offset;
};

// Store all flavors of IO in variant to avoid having to do heap allocations at runtime.
// Size of the variant within IoBase will be the size of the largest IoWrapper<Derived>.
// !!!Important to not create large types!!!
using IoVariant = std::variant<
    std::monostate, // Unset
    IoRead,
    IoWrite>;

class IoBase {
    // Get rid of the state machine, too complex and will lead to deadlock.
    // Copy SimpleAsyncIO with `opsFreeList_`
    // (1) Deque from opsFreeList_
    // (2) Schedule IO, only maintain pointer address (treat as raw trick)
    // (3) re-enqueue on opsFreeList_ when done
    //
    // ?? Can we do better than a queue with a single lock ??
    public:
        enum class State : uint8_t {
            Available,
            Pending,
            Suspended,
            Ready,
        };

    IoBase() : state_(State::Available), res_(std::numeric_limits<int>::min()), io_(std::monostate{}) {}

    template <typename T>
    void setIoVariant(T&& io) {
        io_ = std::move(io);
    }

    void prepSqe(io_uring_sqe* sqe) {
        std::visit([sqe](auto&& arg) {
            using IoType = std::decay_t<decltype(arg)>;
            if constexpr (!std::is_same_v<IoType, std::monostate>) {
                arg.prepSqe(sqe);
            } else {
                // Handle the case where io_ is not set
                throw std::runtime_error("IoVariant is not set");
            }
        }, io_);
    }

    // [DEPRECATED] cqe flags unused
    bool tryMarkReadyAndResume(int res, uint32_t cqeFlags) noexcept {
        res_ = res;

        if (!stateTransition(State::Pending, State::Ready) && stateTransition(State::Suspended, State::Ready)) {
            return true;
        }

        return false;
    }

    // [DEPRECATED] we will never submit/delete inline, always suspend
    bool tryMarkSuspended(std::coroutine_handle<> coroutine) noexcept {
        if (stateTransition(State::Pending, State::Suspended)) {
            coroutine_ = coroutine;
            return true;
        }
        return false;
    }

    bool isReady() const noexcept {
        return state_.load() == State::Ready;
    }

    bool tryAcquire() noexcept {
        return stateTransition(State::Available, State::Pending);
    }

    void markAvailable() noexcept {
        state_.store(State::Available);
        couroutine_ = nullptr;
        res_ = std::numeric_limits<int>::min();
        io_ = std::monostate{};
    }

    int getResult() const noexcept {
        return res_;
    }

    State getState() const noexcept {
        return state_.load();
    }

    private:
        bool stateTransition(State expected, State desired) noexcept {
            return state_.compare_exchange_strong(expected, desired);
        }

        std::atomic<State> state_{State::Available};
        std::coroutine_handle<> coroutine_{nullptr};
        int res_{std::numeric_limits<int>::min()};
        IoVariant io_;
};
