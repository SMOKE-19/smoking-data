use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{sync_channel, Receiver, SyncSender};
use std::sync::Arc;
use std::thread::{self, JoinHandle};

/// Bounded producer-to-consumer bridge used by payload executors.
///
/// The producer retains at most `capacity` queued messages. The consumer owns
/// format-specific writer state and is responsible for checking `commit`
/// before publishing temporary output.
pub(crate) struct BoundedPipeline<T, R> {
    sender: Option<SyncSender<T>>,
    handle: Option<JoinHandle<Result<R, String>>>,
    commit: Arc<AtomicBool>,
    pub(crate) capacity: usize,
}

impl<T, R> BoundedPipeline<T, R>
where
    T: Send + 'static,
    R: Send + 'static,
{
    pub(crate) fn start<F>(name: &str, capacity: usize, consume: F) -> Result<Self, String>
    where
        F: FnOnce(Receiver<T>, Arc<AtomicBool>) -> Result<R, String> + Send + 'static,
    {
        let capacity = capacity.max(1);
        let (sender, receiver) = sync_channel::<T>(capacity);
        let commit = Arc::new(AtomicBool::new(false));
        let consumer_commit = Arc::clone(&commit);
        let handle = thread::Builder::new()
            .name(name.to_string())
            .spawn(move || consume(receiver, consumer_commit))
            .map_err(|error| format!("failed to spawn {name}: {error}"))?;
        Ok(Self {
            sender: Some(sender),
            handle: Some(handle),
            commit,
            capacity,
        })
    }

    pub(crate) fn send(&self, message: T) -> Result<(), String> {
        self.sender
            .as_ref()
            .ok_or_else(|| "bounded pipeline channel is already closed".to_string())?
            .send(message)
            .map_err(|error| format!("bounded pipeline consumer failed: {error}"))
    }

    pub(crate) fn finish(mut self) -> Result<R, String> {
        self.commit.store(true, Ordering::Release);
        self.sender.take();
        let handle = self
            .handle
            .take()
            .ok_or_else(|| "bounded pipeline consumer thread is missing".to_string())?;
        handle
            .join()
            .map_err(|_| "bounded pipeline consumer thread panicked".to_string())?
    }
}

impl<T, R> Drop for BoundedPipeline<T, R> {
    fn drop(&mut self) {
        self.sender.take();
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}
