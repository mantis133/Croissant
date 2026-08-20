use std::any::type_name;
use std::ops::Deref;
use std::sync::Arc;

/// A handle to a service resolved out of the application's
/// [`ServiceRegistry`](crate::application::ServiceRegistry).
///
/// `Injected<T>` derefs to `T`, so an injected service is called like an ordinary field:
///
/// ```
/// # use std::sync::Arc;
/// # use croissant::application::Injected;
/// trait Clock: Send + Sync {
///     fn now(&self) -> u64;
/// }
///
/// struct Fixed(u64);
/// impl Clock for Fixed {
///     fn now(&self) -> u64 {
///         self.0
///     }
/// }
///
/// let clock: Injected<dyn Clock> = Injected::new(Arc::new(Fixed(42)));
/// assert_eq!(clock.now(), 42);
/// ```
///
/// `T` is `?Sized`, so `Injected<dyn Trait>` works and activities can depend on a trait
/// rather than a concrete implementation.
///
/// The default value is unresolved. Dereferencing an unresolved handle panics, naming the
/// service type — that is what a `#[inject]` field with no matching registration looks
/// like at runtime.
pub struct Injected<T: ?Sized> {
    service: Option<Arc<T>>,
    /// The qualifier the field asked for, kept only so a failed deref can say which
    /// registration was missing.
    qualifier: Option<&'static str>,
}

impl<T: ?Sized> Injected<T> {
    /// Wraps an already-resolved service.
    pub fn new(service: Arc<T>) -> Self {
        Injected {
            service: Some(service),
            qualifier: None,
        }
    }

    pub(crate) fn resolved(service: Option<Arc<T>>, qualifier: Option<&'static str>) -> Self {
        Injected { service, qualifier }
    }

    /// An unresolved handle. Dereferencing it panics.
    pub fn missing() -> Self {
        Injected {
            service: None,
            qualifier: None,
        }
    }

    /// Whether a service was actually resolved into this handle.
    pub fn is_present(&self) -> bool {
        self.service.is_some()
    }

    /// The underlying `Arc`, or `None` if unresolved. Use this instead of deref when a
    /// missing service should be handled rather than panic.
    pub fn get(&self) -> Option<&Arc<T>> {
        self.service.as_ref()
    }
}

impl<T: ?Sized> Deref for Injected<T> {
    type Target = T;

    fn deref(&self) -> &T {
        match self.service.as_deref() {
            Some(service) => service,
            None => match self.qualifier {
                Some(qualifier) => panic!(
                    "no service of type `{}` is registered under the qualifier `{qualifier}`",
                    type_name::<T>()
                ),
                None => panic!("no service of type `{}` is registered", type_name::<T>()),
            },
        }
    }
}

impl<T: ?Sized> Default for Injected<T> {
    fn default() -> Self {
        Self::missing()
    }
}

impl<T: ?Sized> Clone for Injected<T> {
    fn clone(&self) -> Self {
        Injected {
            service: self.service.clone(),
            qualifier: self.qualifier,
        }
    }
}

impl<T: ?Sized> std::fmt::Debug for Injected<T> {
    /// Prints the service type rather than the service, which need not be `Debug`.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Injected<{}>(", type_name::<T>())?;
        match (&self.service, self.qualifier) {
            (Some(_), Some(qualifier)) => write!(f, "resolved, qualifier = {qualifier:?}")?,
            (Some(_), None) => write!(f, "resolved")?,
            (None, Some(qualifier)) => write!(f, "missing, qualifier = {qualifier:?}")?,
            (None, None) => write!(f, "missing")?,
        }
        write!(f, ")")
    }
}
