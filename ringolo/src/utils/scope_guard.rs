use std::marker::PhantomData;

/// A guard that executes a closure when it goes out of scope.
///
/// This is a common pattern for ensuring that a piece of code (like resource cleanup)
/// is always executed when a scope is exited, whether by a normal return, an early
/// return, or a panic.
///
/// The guard is "armed" on creation and will execute its closure on drop unless
/// it is explicitly "disarmed".
pub(crate) struct ScopeGuard<'a, F: FnOnce()> {
    // We wrap the closure in an `Option` to allow us to "take" it out when
    // the guard is dropped or disarmed. This ensures the closure is only
    // ever called once and prevents double-panics if the closure itself panics.
    closure: Option<F>,

    _p: PhantomData<&'a ()>,
}

impl<'a, F: FnOnce()> ScopeGuard<'a, F> {
    /// Creates a new, armed `ScopeGuard` with the given closure.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// // We need to bring the struct into scope to use it.
    /// let mut message = String::from("unchanged");
    /// {
    ///     // The guard takes ownership of a closure.
    ///     let _guard = ScopeGuard::new(|| message = String::from("changed!"));
    ///     assert_eq!(message, "unchanged");
    /// } // _guard is dropped here, and the closure runs.
    ///
    /// assert_eq!(message, "changed!");
    /// ```
    pub(crate) fn new(closure: F) -> Self {
        ScopeGuard {
            closure: Some(closure),
            _p: PhantomData,
        }
    }

    /// Disarms the `ScopeGuard`, preventing the closure from being executed on drop.
    ///
    /// After calling this, the guard will do nothing when it goes out of scope.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// let mut message = String::from("unchanged");
    /// {
    ///     let mut guard = ScopeGuard::new(|| message = String::from("changed!"));
    ///     // Some condition is met, so we prevent the cleanup logic.
    ///     guard.disarm();
    /// } // guard is dropped, but does nothing.
    ///
    //  assert_eq!(message, "unchanged");
    /// ```
    pub(crate) fn disarm(&mut self) {
        self.closure.take();
    }
}

impl<'a, F: FnOnce()> Drop for ScopeGuard<'a, F> {
    /// Executes the closure if the guard has not been disarmed.
    fn drop(&mut self) {
        // `self.closure.take()` atomically takes the `Some(value)` out,
        // leaving `None` in its place. This is key to ensuring the closure
        // is only ever called once.
        if let Some(closure) = self.closure.take() {
            closure();
        }
    }
}
