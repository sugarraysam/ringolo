//! Macros for use with Ringolo
use proc_macro2::{Span, TokenStream};
use quote::{quote, quote_spanned};
use syn::parse::Parser;

use crate::parse::*;

// syn::AttributeArgs does not implement syn::Parse
type AttributeArgs = syn::punctuated::Punctuated<syn::Meta, syn::Token![,]>;

/// Config used in case of the attribute not being able to build a valid config
const DEFAULT_ERROR_CONFIG: FinalConfig = FinalConfig {
    flavor: RuntimeFlavor::Stealing,
    worker_threads: None,
};

/// For quickstart on how this works, let's analyze this example:
///
/// ```rust,no_run
/// #[ringolo::main(flavor = "stealing", worker_threads = 2)]
/// async fn main() {
///     println!("Hello world");
/// }
/// ```
///
/// Then the compiler splits the tokens where args is everything bertween the
/// proc_macro parentheses:
/// - `flavor = "stealing", worker_threads = 2`
///
/// And item is the future that is to be invoked by the runtime:
/// ```no_compile
/// async fn main() {
///     println!("Hello world");
/// }
/// ```
pub(crate) fn main(args: TokenStream, item: TokenStream) -> TokenStream {
    // If any of the steps for this macro fail, we still want to expand to an item that is as close
    // to the expected output as possible. This helps out IDEs such that completions and other
    // related features keep working.
    let input: ItemFn = match syn::parse2(item.clone()) {
        Ok(it) => it,
        Err(e) => return token_stream_with_error(item, e),
    };

    let config = if input.sig.ident == "main" && !input.sig.inputs.is_empty() {
        let msg = "the main function cannot accept arguments";
        Err(syn::Error::new_spanned(&input.sig.ident, msg))
    } else {
        AttributeArgs::parse_terminated
            .parse2(args)
            .and_then(|args| build_config(&input, args, false))
    };

    match config {
        Ok(config) => parse_knobs(input, false, config),
        Err(e) => token_stream_with_error(parse_knobs(input, false, DEFAULT_ERROR_CONFIG), e),
    }
}
pub(crate) fn test(args: TokenStream, item: TokenStream) -> TokenStream {
    // If any of the steps for this macro fail, we still want to expand to an item that is as close
    // to the expected output as possible. This helps out IDEs such that completions and other
    // related features keep working.
    let input: ItemFn = match syn::parse2(item.clone()) {
        Ok(it) => it,
        Err(e) => return token_stream_with_error(item, e),
    };
    let config = if let Some(attr) = input.attrs().find(|attr| is_test_attribute(attr)) {
        let msg = "second test attribute is supplied, consider removing or changing the order of your test attributes";
        Err(syn::Error::new_spanned(attr, msg))
    } else {
        AttributeArgs::parse_terminated
            .parse2(args)
            .and_then(|args| build_config(&input, args, true))
    };

    match config {
        Ok(config) => parse_knobs(input, true, config),
        Err(e) => token_stream_with_error(parse_knobs(input, true, DEFAULT_ERROR_CONFIG), e),
    }
}

#[derive(Clone, Copy, PartialEq)]
enum RuntimeFlavor {
    Local,
    Stealing,
}

impl RuntimeFlavor {
    fn from_str(s: &str) -> Result<RuntimeFlavor, String> {
        match s {
            "stealing" => Ok(RuntimeFlavor::Stealing),
            "local" => Ok(RuntimeFlavor::Local),
            _ => Err(format!(
                "No such runtime flavor `{s}`. The runtime flavors are `stealing` and `local`."
            )),
        }
    }
}

struct FinalConfig {
    flavor: RuntimeFlavor,
    worker_threads: Option<usize>,
}

struct ConfigBuilder {
    default_flavor: RuntimeFlavor,
    flavor: Option<RuntimeFlavor>,
    worker_threads: Option<(usize, Span)>,
    is_test: bool,
}

impl ConfigBuilder {
    fn new(is_test: bool) -> Self {
        ConfigBuilder {
            default_flavor: match is_test {
                true => RuntimeFlavor::Local,
                false => RuntimeFlavor::Stealing,
            },
            flavor: None,
            worker_threads: None,
            is_test,
        }
    }

    fn macro_name(&self) -> &'static str {
        if self.is_test {
            "ringolo::test"
        } else {
            "ringolo::main"
        }
    }

    fn set_flavor(&mut self, runtime: syn::Lit, span: Span) -> Result<(), syn::Error> {
        if self.flavor.is_some() {
            return Err(syn::Error::new(span, "`flavor` set multiple times."));
        }

        let runtime_str = parse_string(runtime, span, "flavor")?;
        let runtime =
            RuntimeFlavor::from_str(&runtime_str).map_err(|err| syn::Error::new(span, err))?;
        self.flavor = Some(runtime);

        Ok(())
    }

    fn set_worker_threads(
        &mut self,
        worker_threads: syn::Lit,
        span: Span,
    ) -> Result<(), syn::Error> {
        if self.worker_threads.is_some() {
            return Err(syn::Error::new(
                span,
                "`worker_threads` set multiple times.",
            ));
        }

        let worker_threads = parse_int(worker_threads, span, "worker_threads")?;
        if worker_threads == 0 {
            return Err(syn::Error::new(span, "`worker_threads` may not be 0."));
        }
        self.worker_threads = Some((worker_threads, span));

        Ok(())
    }

    fn build(&self) -> Result<FinalConfig, syn::Error> {
        let flavor = self.flavor.unwrap_or(self.default_flavor);

        let worker_threads = match (flavor, self.worker_threads) {
            (RuntimeFlavor::Local, Some((_, worker_threads_span))) => {
                let msg = format!(
                    "The `worker_threads` option requires the `stealing` runtime flavor. Use `#[{}(flavor = \"stealing\")]`",
                    self.macro_name(),
                );
                return Err(syn::Error::new(worker_threads_span, msg));
            }
            (RuntimeFlavor::Stealing, Some((worker_threads, _))) => Some(worker_threads),
            (_, None) => None,
        };

        Ok(FinalConfig {
            flavor,
            worker_threads,
        })
    }
}

fn build_config(
    input: &ItemFn,
    args: AttributeArgs,
    is_test: bool,
) -> Result<FinalConfig, syn::Error> {
    if input.sig.asyncness.is_none() {
        let msg = "the `async` keyword is missing from the function declaration";
        return Err(syn::Error::new_spanned(input.sig.fn_token, msg));
    }

    let mut config = ConfigBuilder::new(is_test);
    let macro_name = config.macro_name();

    for arg in args {
        match arg {
            syn::Meta::NameValue(namevalue) => {
                let ident = namevalue
                    .path
                    .get_ident()
                    .ok_or_else(|| {
                        syn::Error::new_spanned(&namevalue, "Must have specified ident")
                    })?
                    .to_string()
                    .to_lowercase();
                let lit = match &namevalue.value {
                    syn::Expr::Lit(syn::ExprLit { lit, .. }) => lit,
                    expr => return Err(syn::Error::new_spanned(expr, "Must be a literal")),
                };
                match ident.as_str() {
                    "worker_threads" => {
                        config.set_worker_threads(lit.clone(), syn::spanned::Spanned::span(lit))?;
                    }
                    "flavor" => {
                        config.set_flavor(lit.clone(), syn::spanned::Spanned::span(lit))?;
                    }
                    name => {
                        let msg = format!(
                            "Unknown attribute {name} is specified; expected one of: `flavor`, `worker_threads`, `start_paused`, `crate`, `unhandled_panic`",
                        );
                        return Err(syn::Error::new_spanned(namevalue, msg));
                    }
                }
            }
            syn::Meta::Path(path) => {
                let name = path
                    .get_ident()
                    .ok_or_else(|| syn::Error::new_spanned(&path, "Must have specified ident"))?
                    .to_string()
                    .to_lowercase();
                let msg = match name.as_str() {
                    "threaded_scheduler" | "multi_thread" => {
                        format!(
                            "Set the runtime flavor with #[{macro_name}(flavor = \"multi_thread\")]."
                        )
                    }
                    "basic_scheduler" | "current_thread" | "single_threaded" => {
                        format!(
                            "Set the runtime flavor with #[{macro_name}(flavor = \"current_thread\")]."
                        )
                    }
                    "flavor" | "worker_threads" | "start_paused" | "crate" | "unhandled_panic" => {
                        format!("The `{name}` attribute requires an argument.")
                    }
                    name => {
                        format!(
                            "Unknown attribute {name} is specified; expected one of: `flavor`, `worker_threads`, `start_paused`, `crate`, `unhandled_panic`."
                        )
                    }
                };
                return Err(syn::Error::new_spanned(path, msg));
            }
            other => {
                return Err(syn::Error::new_spanned(
                    other,
                    "Unknown attribute inside the macro",
                ));
            }
        }
    }

    config.build()
}

fn parse_knobs(mut input: ItemFn, is_test: bool, config: FinalConfig) -> TokenStream {
    input.sig.asyncness = None;

    // If type mismatch occurs, the current rustc points to the last statement.
    let (last_stmt_start_span, last_stmt_end_span) = {
        let mut last_stmt = input.stmts.last().cloned().unwrap_or_default().into_iter();

        // `Span` on stable Rust has a limitation that only points to the first
        // token, not the whole tokens. We can work around this limitation by
        // using the first/last span of the tokens like
        // `syn::Error::new_spanned` does.
        let start = last_stmt.next().map_or_else(Span::call_site, |t| t.span());
        let end = last_stmt.last().map_or(start, |t| t.span());
        (start, end)
    };

    let mut rt = match config.flavor {
        RuntimeFlavor::Local => {
            quote_spanned! {last_stmt_start_span=>
                ringolo::runtime::Builder::new_local()
            }
        }
        RuntimeFlavor::Stealing => quote_spanned! {last_stmt_start_span=>
            ringolo::runtime::Builder::new_stealing()
        },
    };

    // Silence these warnings, will use in future to extend supported macro
    // attributes for building the runtime.
    #[allow(unused_mut)]
    let mut checks: Vec<TokenStream> = vec![];
    #[allow(unused_mut)]
    let mut errors: Vec<TokenStream> = vec![];

    if let Some(v) = config.worker_threads {
        rt = quote_spanned! {last_stmt_start_span=> #rt.worker_threads(#v) };
    }

    let generated_attrs = if is_test {
        quote! {
            #[::core::prelude::v1::test]
        }
    } else {
        quote! {}
    };

    let do_checks: TokenStream = checks
        .iter()
        .zip(&errors)
        .map(|(check, error)| {
            quote! {
                #[cfg(not(#check))]
                compile_error!(#error);
            }
        })
        .collect();

    let body_ident = quote! { body };
    // This explicit `return` is intentional. See tokio-rs/tokio#4636
    let last_block = quote_spanned! {last_stmt_end_span=>
        #do_checks

        #[cfg(all(#(#checks),*))]
        #[allow(clippy::expect_used, clippy::diverging_sub_expression, clippy::needless_return, clippy::unwrap_in_result)]
        {
            return #rt
                .try_build()
                .expect("Failed building the Runtime")
                .block_on(#body_ident);
        }

        #[cfg(not(all(#(#checks),*)))]
        {
            panic!("fell through checks")
        }
    };

    let body = input.body();

    // For test functions pin the body to the stack and use `Pin<&mut dyn
    // Future>` to reduce the amount of `Runtime::block_on` (and related
    // functions) copies we generate during compilation due to the generic
    // parameter `F` (the future to block on). This could have an impact on
    // performance, but because it's only for testing it's unlikely to be very
    // large.
    //
    // We don't do this for the main function as it should only be used once so
    // there will be no benefit.

    let body = if is_test {
        let output_type = match &input.sig.output {
            // For functions with no return value syn doesn't print anything,
            // but that doesn't work as `Output` for our boxed `Future`, so
            // default to `()` (the same type as the function output).
            syn::ReturnType::Default => quote! { () },
            syn::ReturnType::Type(_, ret_type) => quote! { #ret_type },
        };
        quote! {
            let mut unpinned = async #body;
            let pinned = std::pin::pin!(unpinned);
            let body: ::core::pin::Pin<&mut dyn ::core::future::Future<Output = #output_type>> = pinned;
        }
    } else {
        quote! {
            let body = async #body;
        }
    };

    input.into_tokens(generated_attrs, body, last_block)
}
