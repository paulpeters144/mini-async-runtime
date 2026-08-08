use crate::runtime_state::TaskId;
use std::cell::RefCell;
use std::collections::VecDeque;
use std::rc::{Rc, Weak};
use std::task::{RawWaker, RawWakerVTable, Waker};

// A `Waker` is an opaque handle to whatever needs to be re-polled. It is
// delivered to futures through `Context` (see `Task::poll`). But `Waker` is a
// zero-sized type: it only carries a raw pointer to "waker data" plus a vtable
// of functions for cloning, waking and dropping that data. So to *create* a
// waker you must supply those pieces yourself via `RawWaker` + `RawWakerVTable`.
//
// Why not use a ready-made waker? The standard `Waker` is designed for
// multi-threaded runtimes: it requires its payload to be `Send + Sync`, and
// sharing state across threads is normally done with `Arc`. Our runtime is
// single-threaded by design, so all shared state lives in an `Rc<RefCell<_>>`
// (see `RuntimeState`). `Arc` would add overhead and force `Send + Sync`
// requirements we don't need. So we hand-build the raw waker so that the
// payload can be an `Rc` instead of an `Arc`; this keeps the runtime `!Send`
// by design (a threading boundary for blocking work is added later).
//
// The waker holds a `Weak` to the shared ready queue (a `VecDeque<TaskId>`),
// not a strong `Rc`. A strong `Rc` from every task's canonical waker back to
// the queue would prevent the queue (and by extension, the runtime state) from
// ever being freed. `Weak` avoids that: when the executor shuts down and drops
// the last strong reference to the queue, wake attempts become no-ops.
//
// Crucially, the waker does NOT depend on `RuntimeState` at all — it only
// knows about a queue. The executor owns `RuntimeState` which holds an `Rc` to
// the same queue; the waker pushes to it without knowing what contains it.
struct WakerData {
    queue: Weak<RefCell<VecDeque<TaskId>>>,
    id: TaskId,
}

const VTABLE: RawWakerVTable = RawWakerVTable::new(clone_raw, wake_raw, wake_by_raw_ref, drop_raw);

fn clone_raw(data: *const ()) -> RawWaker {
    let data = unsafe { Rc::from_raw(data.cast::<WakerData>()) };
    let cloned = data.clone();
    let ptr = Rc::into_raw(cloned).cast::<()>();
    std::mem::forget(data);
    RawWaker::new(ptr, &VTABLE)
}

fn wake_raw(data: *const ()) {
    let data = unsafe { Rc::from_raw(data.cast::<WakerData>()) };
    if let Some(queue) = data.queue.upgrade() {
        queue.borrow_mut().push_back(data.id);
    }
}

fn wake_by_raw_ref(data: *const ()) {
    let data = unsafe { &*(data.cast::<WakerData>()) };
    if let Some(queue) = data.queue.upgrade() {
        queue.borrow_mut().push_back(data.id);
    }
}

fn drop_raw(data: *const ()) {
    drop(unsafe { Rc::from_raw(data.cast::<WakerData>()) });
}

pub fn create_waker(queue: Rc<RefCell<VecDeque<TaskId>>>, id: TaskId) -> Waker {
    let data = Rc::new(WakerData { queue: Rc::downgrade(&queue), id });
    let ptr = Rc::into_raw(data).cast::<()>();
    unsafe { Waker::from_raw(RawWaker::new(ptr, &VTABLE)) }
}

// Each test below exercises one contract the waker must honour. If any of
// these break, the runtime would either lose wakeups (tasks sleeping forever)
// or crash with use-after-free, so these are the most important tests in the
// crate.

// A task has exactly one job: wake itself up. `waker.wake()` is how a running
// future asks to be polled again later. It does this by pushing its id into
// the shared queue; the executor drains that queue to decide what to poll.
// This test checks the whole round trip: create a waker for id TaskId(7), wake it,
// and confirm the id shows up in the queue exactly once.
#[test]
fn wake_push_id() {
    let queue = Rc::new(RefCell::new(VecDeque::new()));
    let waker = create_waker(queue.clone(), TaskId(7));
    waker.wake();
    assert_eq!(queue.borrow().clone(), VecDeque::from([TaskId(7)]));
}

// `wake_by_ref` takes `&self` instead of consuming the waker, so one waker can
// be used many times. An executor usually has a single waker per task but may
// need to wake that task from several places (a socket ready, a timer fired).
// Here we wake twice from the same waker and expect two entries in the queue.
#[test]
fn wake_by_ref_twice() {
    let queue = Rc::new(RefCell::new(VecDeque::new()));
    let waker = create_waker(queue.clone(), TaskId(7));
    waker.wake_by_ref();
    waker.wake_by_ref();
    assert_eq!(
        queue.borrow().clone(),
        VecDeque::from([TaskId(7), TaskId(7)])
    );
}

// Wakers are cheap to clone: a clone does NOT copy the data, it adds one more
// reference to the same underlying `WakerData` (that is what the raw-pointer
// vtable dance in `clone_raw` is for). This lets the runtime hand copies of
// the same waker to multiple places. Here the clone and the original both
// point at id TaskId(7), and waking through either one reaches the same queue.
#[test]
fn cloned_waker_works() {
    let queue = Rc::new(RefCell::new(VecDeque::new()));
    let waker = create_waker(queue.clone(), TaskId(7));
    let c = waker.clone();
    c.wake();
    waker.wake_by_ref();
    assert_eq!(
        queue.borrow().clone(),
        VecDeque::from([TaskId(7), TaskId(7)])
    );
}

// The trickiest part of a waker is memory: every `Rc::into_raw` leaks one
// reference into a raw pointer, and every leaked reference MUST eventually be
// reclaimed with `Rc::from_raw` + drop. If we forget one, we leak memory; if
// we reclaim one twice, we get a use-after-free. This test watches the count
// step by step: leaking into the waker (1 -> 2), cloning (2 -> 3), dropping
// the clone (3 -> 2), dropping the waker (2 -> 1). Landing back on 1, the
// single reference still held by our test variable `data`, proves every clone
// was cleaned up exactly once.
//
// The waker payload holds a `Weak` to the queue, so the strong count of
// `queue` is not affected. The test variable `data` is the `Rc<WakerData>`
// which is what the waker's clone/drop operations manage.
#[test]
fn refcount_stays_balanced() {
    let queue = Rc::new(RefCell::new(VecDeque::new()));
    let data = Rc::new(WakerData { queue: Rc::downgrade(&queue), id: TaskId(1) });
    assert_eq!(Rc::strong_count(&data), 1);

    let waker = unsafe {
        Waker::from_raw(RawWaker::new(
            Rc::into_raw(data.clone()).cast::<()>(),
            &VTABLE,
        ))
    };
    assert_eq!(Rc::strong_count(&data), 2);

    let cloned = waker.clone();
    assert_eq!(Rc::strong_count(&data), 3);

    drop(cloned);
    assert_eq!(Rc::strong_count(&data), 2);

    drop(waker);
    assert_eq!(Rc::strong_count(&data), 1);
}
