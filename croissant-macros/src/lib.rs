//! Derive macro for `croissant::activities::ActivityState`.
//!
//! See the re-export in the `croissant` crate for the user-facing documentation.

use proc_macro::TokenStream;
use proc_macro2::TokenStream as TokenStream2;
use quote::quote;
use syn::parse::{Parse, ParseStream};
use syn::punctuated::Punctuated;
use syn::{
    Attribute, Data, DeriveInput, Fields, Ident, Index, LitStr, Member, Token, parse_macro_input,
    spanned::Spanned,
};

/// Derives `ActivityState`, wiring up `#[global]` and `#[inject]` fields.
///
/// `global` and `inject` are inert helper attributes: the compiler ignores them, so the
/// fields they mark stay ordinary fields of ordinary types.
#[proc_macro_derive(ActivityState, attributes(global, inject))]
pub fn derive_activity_state(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    match expand(input) {
        Ok(tokens) => tokens.into(),
        Err(error) => error.to_compile_error().into(),
    }
}

fn expand(input: DeriveInput) -> syn::Result<TokenStream2> {
    let fields = collect_fields(&input)?;
    let name = &input.ident;
    let (impl_generics, ty_generics, where_clause) = input.generics.split_for_impl();

    let mut injects = Vec::new();
    let mut checkouts = Vec::new();
    let mut checkins = Vec::new();

    for field in &fields {
        match &field.binding {
            Binding::Inject { qualifier } => {
                let member = &field.member;
                let qualifier = match qualifier {
                    Some(qualifier) => quote!(::core::option::Option::Some(#qualifier)),
                    None => quote!(::core::option::Option::None),
                };
                injects.push(quote! {
                    self.#member = services.resolve(#qualifier);
                });
            }
            Binding::Global { key, readonly } => {
                let member = &field.member;
                if *readonly {
                    // Copied in and never written back — that is what read-only means here.
                    checkouts.push(quote! {
                        store.clone_field(#key, &mut self.#member);
                    });
                } else {
                    checkouts.push(quote! {
                        store.checkout_field(#key, &mut self.#member);
                    });
                    checkins.push(quote! {
                        store.checkin_field(#key, &mut self.#member);
                    });
                }
            }
        }
    }

    // Only emit the methods that have work to do, so a state with no attributes gets the
    // trait's no-op defaults rather than three empty overrides.
    let inject_services = (!injects.is_empty()).then(|| {
        quote! {
            fn inject_services(
                &mut self,
                services: &::croissant::application::ServiceRegistry,
            ) {
                #(#injects)*
            }
        }
    });
    let checkout_globals = (!checkouts.is_empty()).then(|| {
        quote! {
            fn checkout_globals(&mut self, store: &mut ::croissant::application::ValueStore) {
                #(#checkouts)*
            }
        }
    });
    let checkin_globals = (!checkins.is_empty()).then(|| {
        quote! {
            fn checkin_globals(&mut self, store: &mut ::croissant::application::ValueStore) {
                #(#checkins)*
            }
        }
    });

    Ok(quote! {
        impl #impl_generics ::croissant::activities::ActivityState
            for #name #ty_generics #where_clause
        {
            #inject_services
            #checkout_globals
            #checkin_globals
        }
    })
}

/// A field carrying one of the two attributes. Unmarked fields are dropped here.
struct BoundField {
    member: Member,
    binding: Binding,
}

enum Binding {
    Global { key: LitStr, readonly: bool },
    Inject { qualifier: Option<LitStr> },
}

fn collect_fields(input: &DeriveInput) -> syn::Result<Vec<BoundField>> {
    let data = match &input.data {
        Data::Struct(data) => data,
        Data::Enum(_) => {
            return Err(syn::Error::new(
                input.ident.span(),
                "ActivityState cannot be derived for an enum; activity state must be a struct",
            ));
        }
        Data::Union(_) => {
            return Err(syn::Error::new(
                input.ident.span(),
                "ActivityState cannot be derived for a union; activity state must be a struct",
            ));
        }
    };

    let fields: Vec<(Member, &syn::Field)> = match &data.fields {
        Fields::Named(named) => named
            .named
            .iter()
            .map(|field| {
                let ident = field.ident.clone().expect("named field has an ident");
                (Member::Named(ident), field)
            })
            .collect(),
        Fields::Unnamed(unnamed) => unnamed
            .unnamed
            .iter()
            .enumerate()
            .map(|(index, field)| {
                (
                    Member::Unnamed(Index {
                        index: index as u32,
                        span: field.span(),
                    }),
                    field,
                )
            })
            .collect(),
        Fields::Unit => Vec::new(),
    };

    let mut bound = Vec::new();
    for (member, field) in fields {
        let global = find_attr(&field.attrs, "global");
        let inject = find_attr(&field.attrs, "inject");

        let binding = match (global, inject) {
            (Some(global), Some(inject)) => {
                let mut error = syn::Error::new(
                    global.span(),
                    "a field cannot be both `#[global]` and `#[inject]`: \
                     `#[global]` is a named value moved in and out of the value store, \
                     `#[inject]` is a shared service resolved by type",
                );
                error.combine(syn::Error::new(inject.span(), "...and `#[inject]` here"));
                return Err(error);
            }
            (Some(global), None) => {
                let GlobalArgs { key, readonly } = parse_global_args(global)?;
                let key = match (key, &member) {
                    (Some(key), _) => key,
                    (None, Member::Named(ident)) => LitStr::new(&ident.to_string(), ident.span()),
                    (None, Member::Unnamed(index)) => {
                        return Err(syn::Error::new(
                            index.span,
                            "a `#[global]` field of a tuple struct has no name to key on; \
                             give it an explicit key, as in `#[global(\"counter\")]`",
                        ));
                    }
                };
                Binding::Global { key, readonly }
            }
            (None, Some(inject)) => Binding::Inject {
                qualifier: parse_inject_args(inject)?,
            },
            (None, None) => continue,
        };

        bound.push(BoundField { member, binding });
    }

    Ok(bound)
}

fn find_attr<'a>(attrs: &'a [Attribute], name: &str) -> Option<&'a Attribute> {
    attrs.iter().find(|attr| attr.path().is_ident(name))
}

#[derive(Default)]
struct GlobalArgs {
    key: Option<LitStr>,
    readonly: bool,
}

/// One argument of `#[global(..)]`: either a string key or the `readonly` marker.
enum GlobalArg {
    Key(LitStr),
    ReadOnly,
}

impl Parse for GlobalArg {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        if input.peek(LitStr) {
            return Ok(GlobalArg::Key(input.parse()?));
        }
        let ident: Ident = input.parse().map_err(|_| {
            input.error("expected a string key or `readonly`, as in `#[global(\"key\", readonly)]`")
        })?;
        if ident == "readonly" {
            Ok(GlobalArg::ReadOnly)
        } else {
            Err(syn::Error::new(
                ident.span(),
                format!("unknown `#[global]` option `{ident}`; expected `readonly`"),
            ))
        }
    }
}

fn parse_global_args(attr: &Attribute) -> syn::Result<GlobalArgs> {
    let mut args = GlobalArgs::default();
    // A bare `#[global]` has no argument list at all.
    if matches!(attr.meta, syn::Meta::Path(_)) {
        return Ok(args);
    }

    let parsed = attr.parse_args_with(Punctuated::<GlobalArg, Token![,]>::parse_terminated)?;
    for arg in parsed {
        match arg {
            GlobalArg::Key(key) => {
                if args.key.is_some() {
                    return Err(syn::Error::new(key.span(), "duplicate `#[global]` key"));
                }
                args.key = Some(key);
            }
            GlobalArg::ReadOnly => args.readonly = true,
        }
    }
    Ok(args)
}

fn parse_inject_args(attr: &Attribute) -> syn::Result<Option<LitStr>> {
    if matches!(attr.meta, syn::Meta::Path(_)) {
        return Ok(None);
    }
    let qualifier: LitStr = attr.parse_args().map_err(|_| {
        syn::Error::new(
            attr.span(),
            "`#[inject]` takes an optional string qualifier, as in `#[inject(\"primary\")]`",
        )
    })?;
    Ok(Some(qualifier))
}
