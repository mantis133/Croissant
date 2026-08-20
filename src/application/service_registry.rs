use std::any::{Any, TypeId};
use std::collections::HashMap;
use std::sync::Arc;

use crate::application::Injected;

/// Registration key: a service type, plus an optional qualifier distinguishing several
/// registrations of that same type.
type ServiceKey = (TypeId, Option<&'static str>);

/// The application's services, keyed by type rather than by name.
///
/// This is the `#[inject]` half of the framework, and the counterpart to
/// [`ValueStore`](crate::application::ValueStore): services are shared `Arc` handles looked
/// up by their type, where the store holds owned values looked up by name. Type keying is
/// what lets an activity depend on `dyn UserRepo` and have a different implementation —
/// including a test fake — supplied at build time.
///
/// Several implementations of one type are told apart by a qualifier:
///
/// ```
/// # use std::sync::Arc;
/// # use croissant::application::ServiceRegistry;
/// trait Store: Send + Sync {
///     fn name(&self) -> &'static str;
/// }
/// struct Postgres;
/// impl Store for Postgres {
///     fn name(&self) -> &'static str { "postgres" }
/// }
/// struct Redis;
/// impl Store for Redis {
///     fn name(&self) -> &'static str { "redis" }
/// }
///
/// let mut services = ServiceRegistry::new();
/// services.register::<dyn Store>(None, Arc::new(Postgres));
/// services.register::<dyn Store>(Some("cache"), Arc::new(Redis));
///
/// assert_eq!(services.resolve::<dyn Store>(None).name(), "postgres");
/// assert_eq!(services.resolve::<dyn Store>(Some("cache")).name(), "redis");
/// ```
#[derive(Default)]
pub struct ServiceRegistry {
    services: HashMap<ServiceKey, Box<dyn Any + Send + Sync>>,
}

impl ServiceRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Registers `service` under its type and an optional qualifier, replacing any earlier
    /// registration for that same pair.
    ///
    /// Name the type explicitly to register behind a trait — `register::<dyn UserRepo>(..)`
    /// is what drives the `Arc<Concrete>` to `Arc<dyn Trait>` coercion at the call site.
    pub fn register<T>(&mut self, qualifier: Option<&'static str>, service: Arc<T>)
    where
        T: ?Sized + Send + Sync + 'static,
    {
        self.services
            .insert((TypeId::of::<T>(), qualifier), Box::new(service));
    }

    /// Looks up a service by type and qualifier.
    ///
    /// A miss is not an error here — it produces an unresolved [`Injected`] that panics only
    /// if something actually dereferences it, which keeps the failure at the use site where
    /// the type name is meaningful.
    pub fn resolve<T>(&self, qualifier: Option<&'static str>) -> Injected<T>
    where
        T: ?Sized + Send + Sync + 'static,
    {
        let service = self
            .services
            .get(&(TypeId::of::<T>(), qualifier))
            .and_then(|service| service.downcast_ref::<Arc<T>>())
            .cloned();
        Injected::resolved(service, qualifier)
    }

    /// Whether a service is registered for this type and qualifier.
    pub fn contains<T>(&self, qualifier: Option<&'static str>) -> bool
    where
        T: ?Sized + Send + Sync + 'static,
    {
        self.services.contains_key(&(TypeId::of::<T>(), qualifier))
    }

    pub fn len(&self) -> usize {
        self.services.len()
    }

    pub fn is_empty(&self) -> bool {
        self.services.is_empty()
    }
}

impl std::fmt::Debug for ServiceRegistry {
    /// Services need not be `Debug`, so this prints the registration keys only.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut keys: Vec<String> = self
            .services
            .keys()
            .map(|(_, qualifier)| match qualifier {
                Some(qualifier) => format!("<service>#{qualifier}"),
                None => "<service>".to_string(),
            })
            .collect();
        keys.sort();
        f.debug_struct("ServiceRegistry")
            .field("count", &self.services.len())
            .field("qualifiers", &keys)
            .finish()
    }
}
