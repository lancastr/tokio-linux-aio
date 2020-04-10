use std::fmt;
use std::ptr::NonNull;


use crossbeam::atomic::AtomicCell;
use intrusive_collections::{DefaultLinkOps, LinkOps};
use intrusive_collections::linked_list::LinkedListOps;

pub struct AtomicLink {
    next: AtomicCell<Option<NonNull<AtomicLink>>>,
    prev: AtomicCell<Option<NonNull<AtomicLink>>>,
}

const UNLINKED_MARKER: Option<NonNull<AtomicLink>> =
    unsafe { Some(NonNull::new_unchecked(1 as *mut AtomicLink)) };

impl AtomicLink {
    /// Creates a new `Link`.
    #[inline]
    pub const fn new() -> AtomicLink {
        AtomicLink {
            next: AtomicCell::new(UNLINKED_MARKER),
            prev: AtomicCell::new(UNLINKED_MARKER),
        }
    }

    /// Checks whether the `Link` is linked into a `LinkedList`.
    #[inline]
    pub fn is_linked(&self) -> bool {
        self.next.load() != UNLINKED_MARKER
    }

    /// Forcibly unlinks an object from a `LinkedList`.
    ///
    /// # Safety
    ///
    /// It is undefined behavior to call this function while still linked into a
    /// `LinkedList`. The only situation where this function is useful is
    /// after calling `fast_clear` on a `LinkedList`, since this clears
    /// the collection without marking the nodes as unlinked.
    #[inline]
    pub unsafe fn force_unlink(&self) {
        self.next.store(UNLINKED_MARKER);
    }
}

impl DefaultLinkOps for AtomicLink {
    type Ops = AtomicLinkOps;

    const NEW: Self::Ops = AtomicLinkOps;
}

// An object containing a link can be sent to another thread if it is unlinked.
unsafe impl Send for AtomicLink {}

// Provide an implementation of Clone which simply initializes the new link as
// unlinked. This allows structs containing a link to derive Clone.
impl Clone for AtomicLink {
    #[inline]
    fn clone(&self) -> AtomicLink {
        AtomicLink::new()
    }
}

// Same as above
impl Default for AtomicLink {
    #[inline]
    fn default() -> AtomicLink {
        AtomicLink::new()
    }
}

// Provide an implementation of Debug so that structs containing a link can
// still derive Debug.
impl fmt::Debug for AtomicLink {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // There isn't anything sensible to print here except whether the link
        // is currently in a list.
        if self.is_linked() {
            write!(f, "linked")
        } else {
            write!(f, "unlinked")
        }
    }
}

// =============================================================================
// LinkOps
// =============================================================================

#[derive(Clone, Copy, Default)]
pub struct AtomicLinkOps;

unsafe impl LinkOps for AtomicLinkOps {
    type LinkPtr = NonNull<AtomicLink>;

    #[inline]
    unsafe fn is_linked(&self, ptr: Self::LinkPtr) -> bool {
        ptr.as_ref().is_linked()
    }

    #[inline]
    unsafe fn mark_unlinked(&mut self, ptr: Self::LinkPtr) {
        ptr.as_ref().next.store(UNLINKED_MARKER);
    }
}

unsafe impl LinkedListOps for AtomicLinkOps {
    #[inline]
    unsafe fn next(&self, ptr: Self::LinkPtr) -> Option<Self::LinkPtr> {
        ptr.as_ref().next.load()
    }

    #[inline]
    unsafe fn prev(&self, ptr: Self::LinkPtr) -> Option<Self::LinkPtr> {
        ptr.as_ref().prev.load()
    }

    #[inline]
    unsafe fn set_next(&mut self, ptr: Self::LinkPtr, next: Option<Self::LinkPtr>) {
        ptr.as_ref().next.store(next);
    }

    #[inline]
    unsafe fn set_prev(&mut self, ptr: Self::LinkPtr, prev: Option<Self::LinkPtr>) {
        ptr.as_ref().prev.store(prev);
    }
}