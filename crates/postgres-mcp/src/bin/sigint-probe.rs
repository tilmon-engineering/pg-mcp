//! Test-only POSIX SIGINT delivery probe.
//!
//! This executable is not invoked by production code. The shutdown integration
//! test uses it to prove `libc::kill(child_pid, SIGINT)` reaches a child process
//! in this runner before asserting MCP-specific cleanup behavior.

#[cfg(unix)]
use std::sync::atomic::{AtomicBool, Ordering};

#[cfg(unix)]
static RECEIVED: AtomicBool = AtomicBool::new(false);

#[cfg(unix)]
extern "C" fn handler(_signal: libc::c_int) {
    RECEIVED.store(true, Ordering::Release);
}

#[cfg(unix)]
fn main() {
    // SAFETY: sigaction storage is initialized before use; the handler performs
    // only an atomic store, which is async-signal-safe.
    let mut action: libc::sigaction = unsafe { std::mem::zeroed() };
    action.sa_sigaction = handler as *const () as usize;
    action.sa_flags = libc::SA_RESTART;
    // SAFETY: pointers reference valid storage.
    unsafe {
        libc::sigemptyset(&mut action.sa_mask);
        if libc::sigaction(libc::SIGINT, &action, std::ptr::null_mut()) != 0 {
            std::process::exit(2);
        }
    }
    println!("ready");
    while !RECEIVED.load(Ordering::Acquire) {
        std::thread::sleep(std::time::Duration::from_millis(1));
    }
}

#[cfg(not(unix))]
fn main() {
    std::process::exit(2);
}
