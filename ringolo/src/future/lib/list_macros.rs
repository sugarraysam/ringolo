/// Creates a `Vec<AnyOp>` from a list of distinct operations.
///
/// This macro handles the conversion of concrete types (like `Bind`, `Listen`)
/// into the `AnyOp` enum variant required by `OpList`.
///
/// # Example
///
/// ```ignore
/// use ringolo::any_vec;
/// use ringolo::future::lib::{Accept, Bind, KernelFdMode, Listen, OpList};
///
/// # async fn doc() -> anyhow::Result<()> {
/// let ops = OpList::new_chain(any_vec![
///     Bind::new(listener_ref, &sock_addr),
///     Listen::new(listener_ref, 128),
///     Accept::new(listener_ref, KernelFdMode::Legacy, true, None),
/// ])
/// .await?;
/// # Ok(())
/// # }
/// ```
#[macro_export]
macro_rules! any_vec{
    ($($op:expr),* $(,)?) => {
        vec![$($op.into()),*]
    };
}

/// Extracts a strongly-typed result from a heterogeneous `AnyOpOutput`.
///
/// # Examples
///
/// ```ignore
/// let mut iter = results.into_iter();
/// let bind_res = any_extract!(iter.next().unwrap(), Bind);
/// let listen_res = any_extract!(iter.next().unwrap(), Listen);
/// ```
///
/// # Panics
///
/// Panics if the runtime type does not match the expected variant. This is generally
/// safe when used with `OpList` because the order of results matches the order of inputs.
#[macro_export]
macro_rules! any_extract {
    ($res:expr, $variant:ident) => {
        match $res {
            Err(e) => Err(e),
            Ok(any_op) => match any_op {
                $crate::future::lib::list::AnyOpOutput::$variant(inner) => Ok(inner),
                other => $crate::future::lib::list_macros::panic_any_extract(format!(
                    "Type mismatch expected {} but got {:?}",
                    stringify!($crate::future::lib::list::AnyOpOutput::$variant),
                    other,
                )),
            },
        }
    };
}

///
/// Extracts a strongly-typed tuple of results from a `Vec<AnyOpOutput>`.
///
/// This macro takes a `Vec<Result<AnyOpOutput, IoError>>` and a list of
/// expected `AnyOpOutput` variants. It returns a tuple where each
/// element is a `Result<InnerType, IoError>`.
///
/// # Examples
///
///```ignore
/// use ringolo::any_extract_all;
/// let (bind_res, listen_res) = any_extract_all!(results, Bind, Listen);
///```
///
/// # Panics
///
/// This macro will panic if:
/// 1. The number of variants passed to the macro does not match the
///    length of the input `Vec`.
/// 2. The type of an operation at a given index does not match the
///    expected variant.
///
#[macro_export]
macro_rules! any_extract_all {
    ($vec:expr, $($variant:ident),+) => {{
        // 1. Compile-time array of stringified variant names
        let expected_len = [$(stringify!($variant)),+].len();
        let vec = $vec; // Take ownership of the vector

        // 2. Runtime length check
        if vec.len() != expected_len {
            $crate::future::lib::list_macros::panic_any_extract(
                format!("expected {} outputs but got {}", expected_len, vec.len())
            );
        }

        // 3. Create an iterator and expand the tuple
        let mut iter = vec.into_iter();

        (
            // 4. Repetition block: repeats for each $variant
            $({
                // Safety: safe because of the length check
                let res = iter.next().unwrap();
                any_extract!(res, $variant)
            }),+
        )
    }};
}

#[allow(unused)]
#[cold]
#[track_caller]
pub(crate) fn panic_any_extract<M>(msg: M) -> !
where
    M: std::fmt::Display,
{
    panic!("FATAL: any_extract error: {msg}.")
}
