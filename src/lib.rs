#[cfg(feature = "crossterm")]
pub mod crossterm;
#[cfg(feature = "logging")]
pub mod logging;
pub mod application;
pub mod activities;
pub mod events;
pub mod tasks;
pub mod streams;



use futures::Stream;
use std::pin::Pin;

pub type EventStream<EventType> = Pin<Box<dyn Stream<Item = EventType> + Send>>;


// #[cfg(test)]
// mod tests {
//     use std::collections::HashMap;

//     struct DataElement {
//         data: Box<[u8]>,
//     }

//     #[test]
//     fn it_works() {
//         let mut data: HashMap<&str, DataElement> = HashMap::new();

//         // assume we want a global int under the name "current time"
//         data.insert("current time", DataElement { data: Box::new(6u32.to_le_bytes()) });

//         // now we pull the data out and cast as a &mut u32
//         let current_time = data.get_mut("current time").unwrap();
//         let current_time: &mut u32 = unsafe { &mut *(current_time.data.as_mut_ptr() as *mut u32) };

//         println!("current time: {}", current_time);

//         *current_time += 1;

//         println!("current time: {}", current_time);

//         let new_current_time = data.get_mut("current time").unwrap();
//         let new_current_time: &mut u32 = unsafe { &mut *(new_current_time.data.as_mut_ptr() as *mut u32) };

//         println!("new current time: {}", new_current_time);
//     }
// }
